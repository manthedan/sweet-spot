"""Object storage for runs: s3:// URIs via boto3, file:// (or bare paths) on the local filesystem.

Local storage exists so `sweetspot local` and the tests exercise the same commit protocol without AWS.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Iterator, Sequence

from botocore.exceptions import ClientError


def split_s3(uri: str) -> tuple[str, str]:
    """Bucket and key, taken literally: keys may contain '#', '?', '%' and spaces (no URL parsing)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    if not bucket:
        raise ValueError(f"not an s3:// URI: {uri!r}")
    return bucket, key


def join(prefix: str, *parts: str) -> str:
    return "/".join([prefix.rstrip("/"), *(p.strip("/") for p in parts if p)])


def is_local(uri: str) -> bool:
    return not uri.startswith(("s3://", "http://", "https://"))


def local_path(uri: str) -> Path:
    """file:///abs/path or a plain path, taken literally (no percent-decoding)."""
    if uri.startswith("file://"):
        rest = uri[len("file://") :]
        if not rest.startswith("/"):
            raise ValueError(f"file:// URIs must be absolute (file:///path): {uri!r}")
        return Path(rest)
    return Path(uri)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _missing(exc: ClientError) -> bool:
    err = exc.response.get("Error", {})
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return str(err.get("Code")) in {"404", "NoSuchKey", "NotFound"} or status == 404


class Store:
    """Dispatches each call on the URI scheme, so one run can mix s3:// and file:// locations."""

    def __init__(self, session: Any = None, s3: Any = None) -> None:
        self._session = session
        self._s3 = s3

    @property
    def s3(self) -> Any:
        if self._s3 is None:
            import boto3

            self._s3 = (self._session or boto3).client("s3")
        return self._s3

    def get(self, uri: str) -> bytes | None:
        if is_local(uri):
            try:
                return local_path(uri).read_bytes()
            except FileNotFoundError:
                return None
        bucket, key = split_s3(uri)
        try:
            return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if _missing(exc):
                return None
            raise

    def exists(self, uri: str) -> bool:
        if is_local(uri):
            return local_path(uri).is_file()
        bucket, key = split_s3(uri)
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if _missing(exc):
                return False
            raise

    def put(self, uri: str, data: bytes, content_type: str = "application/json") -> None:
        if is_local(uri):
            path = local_path(uri)
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
                tmp.write(data)
            os.replace(tmp.name, path)
            return
        bucket, key = split_s3(uri)
        self.s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

    def put_if_absent(self, uri: str, data: bytes) -> bool:
        """Create the object only if it does not exist. Returns False if someone else already created it."""
        if is_local(uri):
            path = local_path(uri)
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
                tmp.write(data)
            try:
                os.link(tmp.name, path)  # atomic create-if-absent
                return True
            except FileExistsError:
                return False
            finally:
                os.unlink(tmp.name)
        bucket, key = split_s3(uri)
        for _ in range(5):
            try:
                self.s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json", IfNoneMatch="*")
                return True
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code"))
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code == "PreconditionFailed" or status == 412:
                    return False
                if code == "ConditionalRequestConflict" or status == 409:
                    continue  # a concurrent conditional write is in flight; retry
                raise
        raise RuntimeError(f"conditional write kept conflicting: {uri}")

    def upload(self, path: Path, uri: str) -> None:
        if is_local(uri):
            dst = local_path(uri)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, dst)
            return
        bucket, key = split_s3(uri)
        self.s3.upload_file(str(path), bucket, key)

    def download(self, uri: str, path: Path) -> dict[str, Any]:
        """Fetch uri to path; return what identifies the bytes we got (for provenance in done markers)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        if uri.startswith(("http://", "https://")):
            with urllib.request.urlopen(uri, timeout=60) as resp, path.open("wb") as out:  # noqa: S310 - trusted task inputs
                shutil.copyfileobj(resp, out)
                etag = resp.headers.get("ETag")
            return {"uri": uri, "size": path.stat().st_size, **({"etag": etag} if etag else {})}
        if is_local(uri):
            shutil.copyfile(local_path(uri), path)
            return {"uri": uri, "size": path.stat().st_size}
        # One GET, so the recorded identity (ETag, version) is that of the bytes we actually received. No
        # VersionId in the request: that would need s3:GetObjectVersion.
        bucket, key = split_s3(uri)
        resp = self.s3.get_object(Bucket=bucket, Key=key)
        with path.open("wb") as out:
            for chunk in resp["Body"].iter_chunks(1 << 20):
                out.write(chunk)
        ident: dict[str, Any] = {"uri": uri, "size": path.stat().st_size, "etag": str(resp.get("ETag", "")).strip('"')}
        if resp.get("VersionId") and resp["VersionId"] != "null":
            ident["version_id"] = resp["VersionId"]
        return ident

    def sha256(self, uri: str) -> str:
        """sha256 of the object's bytes, streamed."""
        if is_local(uri):
            return sha256_file(local_path(uri))
        bucket, key = split_s3(uri)
        h = hashlib.sha256()
        for chunk in self.s3.get_object(Bucket=bucket, Key=key)["Body"].iter_chunks(1 << 20):
            h.update(chunk)
        return h.hexdigest()

    def copy(self, src: str, dst: str) -> None:
        if is_local(src) and is_local(dst):
            d = local_path(dst)
            d.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path(src), d)
            return
        if is_local(src) or is_local(dst):
            raise ValueError(f"cannot copy between local and remote storage: {src} -> {dst}")
        sb, sk = split_s3(src)
        db, dk = split_s3(dst)
        self.s3.copy({"Bucket": sb, "Key": sk}, db, dk)  # managed copy: multipart above 5 GB

    def list(self, prefix: str) -> Iterator[tuple[str, int, float]]:
        """Yield (uri, size, mtime) for every object under prefix."""
        if is_local(prefix):
            root = local_path(prefix)
            if not root.is_dir():
                return
            for path in sorted(root.rglob("*")):
                if path.is_file():
                    st = path.stat()
                    yield (f"file://{path}" if prefix.startswith("file://") else str(path)), st.st_size, st.st_mtime
            return
        bucket, key = split_s3(prefix)
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key):
            for obj in page.get("Contents", []):
                yield f"s3://{bucket}/{obj['Key']}", int(obj["Size"]), obj["LastModified"].timestamp()

    def delete(self, uri: str) -> None:
        if is_local(uri):
            local_path(uri).unlink(missing_ok=True)
            return
        bucket, key = split_s3(uri)
        self.s3.delete_object(Bucket=bucket, Key=key)

    def delete_many(self, uris: Sequence[str]) -> None:
        local = [u for u in uris if is_local(u)]
        for uri in local:
            local_path(uri).unlink(missing_ok=True)
        by_bucket: dict[str, list[str]] = {}
        for uri in uris:
            if not is_local(uri):
                bucket, key = split_s3(uri)
                by_bucket.setdefault(bucket, []).append(key)
        for bucket, keys in by_bucket.items():
            for i in range(0, len(keys), 1000):
                resp = self.s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]], "Quiet": True})
                if resp.get("Errors"):
                    raise RuntimeError(f"failed to delete {len(resp['Errors'])} objects, e.g. {resp['Errors'][0]}")
