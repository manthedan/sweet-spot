from __future__ import annotations

from pathlib import Path
from time import sleep
from urllib.parse import urlparse

from botocore.exceptions import ClientError


CONDITIONAL_PUT_MAX_ATTEMPTS = 4
CONDITIONAL_PUT_RETRY_SLEEP_SECONDS = 0.25


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


def s3_upload_text_if_absent(
    s3,
    text: str,
    uri: str,
    content_type: str = "application/json",
    *,
    max_attempts: int = CONDITIONAL_PUT_MAX_ATTEMPTS,
    retry_sleep_seconds: float = CONDITIONAL_PUT_RETRY_SLEEP_SECONDS,
) -> bool:
    bucket, key = parse_s3_uri(uri)
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    for attempt in range(max_attempts):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type, IfNoneMatch="*")
            return True
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "PreconditionFailed" or status == 412:
                return False
            if code == "ConditionalRequestConflict" or status == 409:
                if attempt == max_attempts - 1:
                    raise
                if retry_sleep_seconds > 0:
                    sleep(retry_sleep_seconds)
                continue
            raise
    raise AssertionError("unreachable conditional put loop exit")


def s3_download_text(s3, uri: str) -> str:
    bucket, key = parse_s3_uri(uri)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def s3_delete(s3, uri: str) -> None:
    bucket, key = parse_s3_uri(uri)
    s3.delete_object(Bucket=bucket, Key=key)


def s3_upload_file(s3, path: Path, uri: str, content_type: str | None = None, metadata: dict[str, str] | None = None) -> None:
    bucket, key = parse_s3_uri(uri)
    extra: dict[str, object] = {}
    if content_type:
        extra["ContentType"] = content_type
    if metadata:
        extra["Metadata"] = metadata
    if extra:
        s3.upload_file(str(path), bucket, key, ExtraArgs=extra)
    else:
        s3.upload_file(str(path), bucket, key)


def s3_head_object(s3, uri: str) -> dict[str, object]:
    bucket, key = parse_s3_uri(uri)
    return s3.head_object(Bucket=bucket, Key=key)
