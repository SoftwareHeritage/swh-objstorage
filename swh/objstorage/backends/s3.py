# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import hashlib
import logging
from typing import Dict, Optional, Self

from aioaws.core import AwsClient, AWSv4Auth, RequestError
from aioaws.s3 import S3Client, S3Config
from httpx2 import URL, AsyncClient, Response

from swh.objstorage.constants import LiteralPrimaryHash
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import HashDict
from swh.objstorage.objstorage import CompressionFormat, ObjStorage, timed
from swh.objstorage.utils import AsyncLoop

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
        workaround_moto: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.workaround_moto = workaround_moto

        self._async_loop = AsyncLoop()
        self.http_client = self._async_loop.run_until_complete(
            self._async_loop.enter_async_context(AsyncClient())
        )

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
        config = S3Config(key, secret, region, container_name, host)
        self.auth = AWSv4Auth(
            aws_secret_key=secret,
            aws_access_key=key,
            region=region,
            service="s3",
        )
        self.aws_client = AwsClient(self.http_client, config, "s3")
        self.s3_client = S3Client(self.http_client, config)
        if not secure:
            assert self.aws_client.schema == "https"
            assert self.s3_client._aws_client.schema == "https"
            self.aws_client.schema = "http"
            self.s3_client._aws_client.schema = "http"

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
    ) -> Response:
        return await self.http_client.request(
            method,
            url,
            headers={
                **self.auth.auth_headers(
                    method,  # type: ignore[arg-type]  # expects Literal['GET', 'POST']
                    URL(url),
                    data=content,
                    content_type=headers.get("content-type"),
                ),
                **headers,
            },
            content=content,
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
        if r.status_code == 404:
            return False
        return True

    @timed
    def add(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        self._async_loop.run_until_complete(
            self._add_async(content, obj_id, check_presence)
        )

    async def _add_async(
        self, content: bytes, obj_id: HashDict, check_presence: bool = True
    ) -> None:
        if check_presence and await self._contains_async(obj_id):
            return

        def hash_content():
            return hashlib.md5(content, usedforsecurity=False).hexdigest()

        r = await self._request(
            "PUT",
            self._object_url(obj_id),
            headers={"content-type": "application/octet-stream"},
            content=content,
        )
        if r.status_code != 200:
            raise Exception(f"Unexpected POST status code: {r}")

    @timed
    def get(self, obj_id: HashDict) -> bytes:
        obj = self._async_loop.run_until_complete(self._get_async(obj_id))
        return self.decompress(obj, self._internal_id(obj_id))

    async def _get_async(self, obj_id: HashDict) -> bytes:
        path = self._object_path(obj_id)
        try:
            r = await self.aws_client.get(path)
            return r.read()
        except RequestError as e:
            if e.status == 404:
                raise ObjNotFoundError(obj_id) from None
            raise

    def download_url(
        self,
        obj_id: HashDict,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)

        return self.s3_client.signed_download_url(
            self._object_path(obj_id).removeprefix("/"),
            max_age=int(expiry.total_seconds()) if expiry is not None else 24 * 3600,
        )

    def delete(self, obj_id: HashDict):
        super().delete(obj_id)  # Check delete permission

        return self._async_loop.run_until_complete(self._delete_async(obj_id))

    async def _delete_async(self, obj_id: HashDict) -> bool:
        if not await self._contains_async(obj_id):
            raise ObjNotFoundError(obj_id)

        url = URL(self._object_url(obj_id))
        headers = self.auth.auth_headers(
            "DELETE",  # type: ignore[arg-type]  # expects Literal["GET", "POST"]
            url,
        )

        r = await self.http_client.request(
            "DELETE",
            url,
            headers=headers,
        )

        if r.status_code != 204:
            raise Exception(f"Unexpected DELETE status code: {r}")
        return True
