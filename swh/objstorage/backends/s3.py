# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
from datetime import datetime, timezone
import hashlib
import logging
from typing import AsyncIterator, Dict, Iterator, List, Optional, Self, Tuple

import aiohttp

from swh.objstorage.aioaws import AWSv4Auth
from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import HashDict
from swh.objstorage.objstorage import CompressionFormat, ObjStorage, timed
from swh.objstorage.utils import AsyncLoop, add_batch_async

logger = logging.getLogger(__name__)


def now():
    return datetime.now(tz=timezone.utc)


class S3CloudObjStorage(ObjStorage):
    """Amazon's S3 Cloud-based object storage"""

    name: str = "s3"
    primary_hash: LiteralPrimaryHash = "sha1"

    def __init__(
        self,
        container_name: str,
        key: str,
        secret: str,
        region: str = "us-east-1",
        host: str = "s3.amazonaws.com",
        port: Optional[int] = None,
        secure: bool = True,
        compression: CompressionFormat | None = None,
        path_prefix: Optional[str] = None,
        connection_limit: int = 30,  # avg batch size + 20%
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._async_loop = AsyncLoop()

        if host is None:
            host = f"s3.{region}.amazonaws.com"
            if port is not None:
                host = f"{host}:{port}"
        else:
            if port is not None:
                host = f"{host}:{port}"
        self._host = host
        self._container = container_name
        self._secure = secure
        if key is None:
            if secret is not None:
                raise ValueError("key is None, but secret is not")
            self.auth: AWSv4Auth | None = None
        else:
            if secret is None:
                raise ValueError("secret is None, but key is not")
            self.auth = AWSv4Auth(
                aws_secret_key=secret,
                aws_access_key=key,
                region=region,
                service="s3",
            )
        if compression is None:
            logger.warning(
                "Deprecated: compression is undefined. "
                "Defaulting to gzip, but please set it explicitly."
            )
            compression = "gzip"
        self.compression = compression
        if path_prefix:
            self.path_prefix = path_prefix.rstrip("/") + "/"
        else:
            self.path_prefix = ""

        async def create_connector() -> aiohttp.TCPConnector:
            # aiohttp.TCPConnector is a sync function, but it needs to be initialized
            # while an event loop is running
            return aiohttp.TCPConnector(limit=connection_limit)

        connector = self._async_loop.run_until_complete(create_connector())
        self._session = self._async_loop.run_until_complete(
            self._async_loop.enter_async_context(
                aiohttp.ClientSession(connector=connector)
            )
        )

        self._entered = False

    def __enter__(self) -> Self:
        if self._entered:
            raise RuntimeError(
                f"{self.__class__.__name__} is not a re-entrant context manager"
            )
        self._entered = True
        return self

    def __exit__(self, *exc_details):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if self._async_loop.is_alive():
            self._async_loop.terminate()
            assert not self._async_loop.is_alive()

    def _internal_id(self, obj_id: HashDict) -> str:
        """Internal id is the hex version in objstorage."""
        primary_hash = obj_id[self.primary_hash]
        return primary_hash.hex()

    def _object_path(self, obj_id: HashDict) -> str:
        return "/" + self._container + "/" + self._object_key(obj_id)

    def _object_key(self, obj_id: HashDict) -> str:
        return self.path_prefix + self._internal_id(obj_id)

    def _object_url(self, obj_id: HashDict) -> str:
        scheme = "https" if self._secure else "http"
        return scheme + "://" + self._host + self._object_path(obj_id)

    async def _request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str] = {},
        content: Optional[bytes] = None,
    ) -> aiohttp.ClientResponse:
        if self.auth is not None:
            headers = {
                **self.auth.auth_headers(method, url, data=content, headers=headers),
                **headers,
            }
        return await self._session.request(
            method,
            url,
            headers=headers,
            data=content,
        )

    def check_config(self, *, check_write):
        """Check the configuration for this object storage"""
        # FIXME: hopefully this blew up during instantiation
        return True

    @timed
    def __contains__(self, obj_id: HashDict) -> bool:
        return self._async_loop.run_until_complete(self._contains_async(obj_id))

    async def _contains_async(self, obj_id: HashDict) -> bool:
        r = await self._request("HEAD", self._object_url(obj_id))
        return r.status == 200

    @timed
    def add(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        content = self.compress(content)
        self._async_loop.run_until_complete(
            self._add_async(content, obj_id, check_presence)
        )

    async def _add_async(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> Tuple[int, int]:
        if check_presence and await self._contains_async(obj_id):
            return (0, 0)

        def hash_content():
            return hashlib.md5(content, usedforsecurity=False).hexdigest()

        r = await self._request(
            "PUT",
            self._object_url(obj_id),
            headers={"content-type": "application/octet-stream"},
            content=content,
        )
        if r.status != 200:
            raise Exception(f"Unexpected PUT status code: {r}")

        return (1, len(content))

    @timed
    def add_batch(
        self, contents: List[Tuple[HashDict, bytes]], check_presence: bool = True
    ) -> Dict:
        return self._async_loop.run_until_complete(
            add_batch_async(self._add_async, contents, check_presence)
        )

    @timed
    def get(self, obj_id: HashDict) -> bytes:
        obj = self._async_loop.run_until_complete(self._get_async(obj_id))
        return self.decompress(obj, self._internal_id(obj_id))

    async def _get_async(self, obj_id: HashDict) -> bytes:
        url = self._object_url(obj_id)
        r = await self._request("GET", url)
        if r.status == 404:
            raise ObjNotFoundError(obj_id)
        elif r.status == 200:
            return await r.read()
        else:
            raise Exception("Unexpected GET status code: {r}")

    async def _get_async_or_none(self, obj_id) -> Optional[Tuple[HashDict, bytes]]:
        """Like ``get_async(obj_id)``, but returns tuples with the hash dict, and
        returns None instead of raising ResourceNotFoundError.
        Used by ``get_batch`` so other blobs can be returned even if one is missing."""
        try:
            return (obj_id, await self._get_async(obj_id))
        except ObjNotFoundError:
            return None

    @timed
    def get_batch(self, obj_ids: List[HashDict]) -> Iterator[Tuple[HashDict, bytes]]:
        """Retrieve objects' raw content in bulk from storage, concurrently."""
        for obj_id, obj in self._async_loop.iter_asynciterator(
            self._get_batch_async(obj_ids)
        ):
            yield (obj_id, self.decompress(obj, self._internal_id(obj_id)))

    async def _get_batch_async(self, obj_ids) -> AsyncIterator[Tuple[HashDict, bytes]]:
        for fut in asyncio.as_completed(
            [self._get_async_or_none(obj_id) for obj_id in obj_ids]
        ):
            item = await fut
            if item is not None:
                yield item

    def delete(self, obj_id: HashDict):
        super().delete(obj_id)  # Check delete permission

        return self._async_loop.run_until_complete(self._delete_async(obj_id))

    async def _delete_async(self, obj_id: HashDict) -> bool:
        if not await self._contains_async(obj_id):
            raise ObjNotFoundError(obj_id)

        r = await self._request("DELETE", self._object_url(obj_id))
        if r.status != 204:
            raise Exception(f"Unexpected DELETE status code: {r}")
        return True
