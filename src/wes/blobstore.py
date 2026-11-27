"""Content-addressed blob storage for asset bytes.

Objects are stored at `tenants/<tenant>/sha256/<aa>/<bb>/<sha256>`. The key is
derived from the bytes, so writing the same content twice is a no-op and a
retried step can never leave a duplicate behind. Tenants never share objects,
which keeps deletion and retention per tenant simple.

Two backends: a local directory (development, tests) and S3 or anything that
speaks its API, such as MinIO (docker-compose).
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator, Protocol

CHUNK = 1024 * 1024
TENANT_SAFE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


class BlobTooLarge(Exception):
    pass


@dataclass(frozen=True)
class Spooled:
    """Bytes hashed into a local temp file, ready to upload."""

    path: Path
    sha256: str
    size: int

    def cleanup(self) -> None:
        self.path.unlink(missing_ok=True)


def spool(data: bytes | BinaryIO | os.PathLike | str, max_bytes: int | None = None) -> Spooled:
    """Copy `data` to a temp file while hashing it. Paths are read, not moved."""
    fd, tmp = tempfile.mkstemp(prefix="wes-blob-")
    digest, size = hashlib.sha256(), 0
    try:
        with os.fdopen(fd, "wb") as out:
            for chunk in _chunks(data):
                size += len(chunk)
                if max_bytes is not None and size > max_bytes:
                    raise BlobTooLarge(f"content exceeds {max_bytes} bytes")
                digest.update(chunk)
                out.write(chunk)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return Spooled(Path(tmp), digest.hexdigest(), size)


def _chunks(data: bytes | BinaryIO | os.PathLike | str) -> Iterator[bytes]:
    if isinstance(data, (bytes, bytearray, memoryview)):
        yield bytes(data)
        return
    if isinstance(data, (str, os.PathLike)):
        with open(data, "rb") as f:
            yield from iter(lambda: f.read(CHUNK), b"")
        return
    yield from iter(lambda: data.read(CHUNK), b"")


def object_key(tenant_id: str, sha256: str) -> str:
    if not TENANT_SAFE.match(tenant_id):
        raise ValueError(f"tenant id {tenant_id!r} is not safe to use in a storage key")
    return f"tenants/{tenant_id}/sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}"


class BlobStore(Protocol):
    scheme: str

    def uri_for(self, tenant_id: str, sha256: str) -> str:
        """Where content with this hash lives (or will live)."""

    def upload(self, spooled: Spooled, tenant_id: str) -> str:
        """Store spooled bytes; return the object URI. Idempotent."""

    def open(self, uri: str) -> BinaryIO: ...

    def exists(self, uri: str) -> bool: ...

    def delete(self, uri: str) -> None: ...


class LocalBlobStore:
    scheme = "file"

    def __init__(self, root: str | os.PathLike) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def uri_for(self, tenant_id: str, sha256: str) -> str:
        return f"file://{self.root / object_key(tenant_id, sha256)}"

    def upload(self, spooled: Spooled, tenant_id: str) -> str:
        key = object_key(tenant_id, spooled.sha256)
        dest = self.root / key
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Copy to a sibling temp name, then rename: readers never see a partial object.
            tmp = dest.with_name(f".{dest.name}.{os.getpid()}.{id(spooled)}.tmp")
            shutil.copyfile(spooled.path, tmp)
            os.replace(tmp, dest)
        return f"file://{dest}"

    def _path(self, uri: str) -> Path:
        if not uri.startswith("file://"):
            raise ValueError(f"not a local blob uri: {uri}")
        path = Path(uri[len("file://"):]).resolve()
        if self.root not in path.parents:
            raise ValueError(f"blob uri outside the store root: {uri}")
        return path

    def open(self, uri: str) -> BinaryIO:
        return open(self._path(uri), "rb")

    def exists(self, uri: str) -> bool:
        return self._path(uri).exists()

    def delete(self, uri: str) -> None:
        self._path(uri).unlink(missing_ok=True)


class S3BlobStore:
    """S3 or an S3-compatible service (MinIO). Needs the `s3` extra (boto3)."""

    scheme = "s3"

    def __init__(self, bucket: str, *, endpoint_url: str | None = None, region: str | None = None,
                 create_bucket: bool = False):
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - depends on install
            raise RuntimeError("S3 blob storage needs boto3: pip install '.[s3]'") from exc
        self.bucket = bucket
        self.client = boto3.client("s3", endpoint_url=endpoint_url, region_name=region)
        if create_bucket:
            self.ensure_bucket()

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist (development setups such as MinIO)."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as exc:  # another process won the race
                if exc.response.get("Error", {}).get("Code") not in (
                    "BucketAlreadyOwnedByYou", "BucketAlreadyExists"
                ):
                    raise

    def uri_for(self, tenant_id: str, sha256: str) -> str:
        return f"s3://{self.bucket}/{object_key(tenant_id, sha256)}"

    def upload(self, spooled: Spooled, tenant_id: str) -> str:
        key = object_key(tenant_id, spooled.sha256)
        uri = f"s3://{self.bucket}/{key}"
        if not self.exists(uri):
            with open(spooled.path, "rb") as f:
                self.client.put_object(
                    Bucket=self.bucket, Key=key, Body=f,
                    Metadata={"sha256": spooled.sha256},
                )
        return uri

    def _key(self, uri: str) -> str:
        prefix = f"s3://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError(f"blob uri not in bucket {self.bucket}: {uri}")
        return uri[len(prefix):]

    def open(self, uri: str) -> BinaryIO:
        return self.client.get_object(Bucket=self.bucket, Key=self._key(uri))["Body"]

    def exists(self, uri: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(uri))
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def delete(self, uri: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(uri))


def make_blobstore(
    url: str, *, s3_endpoint: str | None = None, s3_region: str | None = None,
    s3_create_bucket: bool = False,
) -> BlobStore:
    """`file:///abs/path`, `file://relative/path`, a bare path, or `s3://bucket`."""
    if url.startswith("s3://"):
        bucket = url[len("s3://"):].strip("/")
        if not bucket or "/" in bucket:
            raise ValueError("WES_BLOB_URL for S3 must be s3://<bucket>")
        return S3BlobStore(bucket, endpoint_url=s3_endpoint, region=s3_region,
                           create_bucket=s3_create_bucket)
    return LocalBlobStore(url[len("file://"):] if url.startswith("file://") else url)
