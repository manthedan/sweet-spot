from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from botocore.exceptions import ClientError


def parse_s3_uri(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    if p.scheme != "s3" or not p.netloc or not p.path:
        raise ValueError(f"not an s3 uri: {uri!r}")
    return p.netloc, p.path.lstrip("/")


def s3_join(prefix: str, *parts: str) -> str:
    return "/".join([prefix.rstrip("/"), *[p.strip("/") for p in parts if p]])


def s3_exists(s3, uri: str) -> bool:
    bucket, key = parse_s3_uri(uri)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_upload_text(s3, text: str, uri: str, content_type: str = "application/json") -> None:
    bucket, key = parse_s3_uri(uri)
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)


def s3_download_text(s3, uri: str) -> str:
    bucket, key = parse_s3_uri(uri)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def s3_upload_file(s3, path: Path, uri: str, content_type: str | None = None) -> None:
    bucket, key = parse_s3_uri(uri)
    extra = {"ContentType": content_type} if content_type else None
    if extra:
        s3.upload_file(str(path), bucket, key, ExtraArgs=extra)
    else:
        s3.upload_file(str(path), bucket, key)
