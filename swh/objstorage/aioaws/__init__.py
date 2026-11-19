"""
Adapted from
https://github.com/pydantic/aioaws/blob/386147ca9fd7b641b88b9a793a91e6856328faf5/aioaws/core.py#L149-L230
and
https://github.com/pydantic/aioaws/blob/386147ca9fd7b641b88b9a793a91e6856328faf5/aioaws/core.py#L259-L268
to remove the dependency on httpx2 and overly strict typing constraints
"""

import base64
from binascii import hexlify
from datetime import datetime, timezone
from functools import reduce
import hashlib
import hmac
from urllib.parse import quote as url_quote
from urllib.parse import urlsplit

_AWS_AUTH_REQUEST = "aws4_request"
_CONTENT_TYPE = "application/x-www-form-urlencoded"
_AUTH_ALGORITHM = "AWS4-HMAC-SHA256"


class AWSv4Auth:
    def __init__(
        self,
        aws_secret_key: str,
        aws_access_key: str,
        region: str,
        service: str,
    ) -> None:
        self.aws_secret_key = aws_secret_key
        self.aws_access_key = aws_access_key
        self.region = region
        self.service = service

    def auth_headers(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None,
    ) -> dict[str, str]:
        _, host, path, query, _ = urlsplit(url)
        now = datetime.now(tz=timezone.utc)
        data = data or b""

        headers = headers or {}
        headers = {k.lower(): v for (k, v) in headers.items()}
        # WARNING! order is important here, headers need to be in alphabetical order
        headers = {
            "content-md5": base64.b64encode(hashlib.md5(data).digest()).decode(),
            "content-type": _CONTENT_TYPE,  # default value
            "host": host,
            "x-amz-date": _aws4_x_amz_date(now),
            **headers,
        }
        headers = dict(sorted(headers.items()))

        payload_sha256_hash = hashlib.sha256(data).hexdigest()
        signed_headers, signature = self.aws4_signature(
            now, method, path, query, headers, payload_sha256_hash
        )
        credential = self.aws4_credential(now)
        authorization_header = (
            f"{_AUTH_ALGORITHM} "
            f"Credential={credential},SignedHeaders={signed_headers},Signature={signature}"
        )
        headers.update(
            {
                "authorization": authorization_header,
                "x-amz-content-sha256": payload_sha256_hash,
            }
        )
        return headers

    def aws4_signature(
        self,
        dt: datetime,
        method: str,
        path: str,
        query: str,
        headers: dict[str, str],
        payload_hash: str,
    ) -> tuple[str, str]:
        header_keys = sorted(headers)
        signed_headers = ";".join(header_keys)
        canonical_request_parts = (
            method,
            url_quote(path),
            query,
            "".join(f"{k}:{headers[k]}\n" for k in header_keys),
            signed_headers,
            payload_hash,
        )
        canonical_request = "\n".join(canonical_request_parts)
        string_to_sign_parts = (
            _AUTH_ALGORITHM,
            _aws4_x_amz_date(dt),
            self._aws4_scope(dt),
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        )
        string_to_sign = "\n".join(string_to_sign_parts)
        return signed_headers, self.aws4_sign_string(string_to_sign, dt)

    def aws4_sign_string(self, string_to_sign: str, dt: datetime) -> str:
        key_parts = (
            b"AWS4" + self.aws_secret_key.encode(),
            _aws4_date_stamp(dt),
            self.region,
            self.service,
            _AWS_AUTH_REQUEST,
            string_to_sign,
        )
        signature_bytes: bytes = reduce(
            _aws4_reduce_signature, key_parts  # type: ignore[arg-type]
        )  # type: ignore[assignment]
        return hexlify(signature_bytes).decode()

    def _aws4_scope(self, dt: datetime) -> str:
        return (
            f"{_aws4_date_stamp(dt)}/{self.region}/{self.service}/{_AWS_AUTH_REQUEST}"
        )

    def aws4_credential(self, dt: datetime) -> str:
        return f"{self.aws_access_key}/{self._aws4_scope(dt)}"


def _aws4_date_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _aws4_x_amz_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _aws4_reduce_signature(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()
