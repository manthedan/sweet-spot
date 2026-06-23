from __future__ import annotations

import sys
import types
import unittest

try:
    from botocore.exceptions import ClientError as _ClientError
except ModuleNotFoundError:

    class _ClientError(Exception):
        def __init__(self, response, operation_name):
            super().__init__(f"{operation_name}: {response}")
            self.response = response

    botocore = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = _ClientError
    botocore.exceptions = exceptions
    sys.modules.setdefault("botocore", botocore)
    sys.modules.setdefault("botocore.exceptions", exceptions)

try:
    from botocore.session import Session
except ModuleNotFoundError:
    Session = None  # type: ignore[assignment]

from sweetspot.s3util import s3_upload_text_if_absent

ClientError = _ClientError


class ConditionalPutContractTests(unittest.TestCase):
    def test_botocore_put_object_supports_if_none_match(self) -> None:
        if Session is None:
            self.skipTest("botocore is not installed")
        op = Session().get_service_model("s3").operation_model("PutObject")
        self.assertIn("IfNoneMatch", op.input_shape.members)

    def test_precondition_failed_means_existing_marker(self) -> None:
        class S3:
            def put_object(self, **_kwargs):
                raise ClientError({"Error": {"Code": "PreconditionFailed"}, "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")

        self.assertFalse(s3_upload_text_if_absent(S3(), "{}", "s3://bucket/key", retry_sleep_seconds=0))

    def test_conditional_conflict_retries_before_success(self) -> None:
        class S3:
            def __init__(self) -> None:
                self.calls = 0
                self.kwargs = []

            def put_object(self, **kwargs):
                self.calls += 1
                self.kwargs.append(kwargs)
                if self.calls == 1:
                    raise ClientError({"Error": {"Code": "ConditionalRequestConflict"}, "ResponseMetadata": {"HTTPStatusCode": 409}}, "PutObject")
                return {}

        s3 = S3()
        self.assertTrue(s3_upload_text_if_absent(s3, "{}", "s3://bucket/key", retry_sleep_seconds=0))
        self.assertEqual(s3.calls, 2)
        self.assertEqual(s3.kwargs[0]["IfNoneMatch"], "*")

    def test_conditional_conflict_exhaustion_raises(self) -> None:
        class S3:
            def put_object(self, **_kwargs):
                raise ClientError({"Error": {"Code": "ConditionalRequestConflict"}, "ResponseMetadata": {"HTTPStatusCode": 409}}, "PutObject")

        with self.assertRaises(ClientError):
            s3_upload_text_if_absent(S3(), "{}", "s3://bucket/key", max_attempts=2, retry_sleep_seconds=0)


if __name__ == "__main__":
    unittest.main()
