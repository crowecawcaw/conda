# Copyright (C) 2012 Anaconda, Inc
# SPDX-License-Identifier: BSD-3-Clause
"""S3 transport adapter with CRT acceleration support."""

from __future__ import annotations

import os
import tempfile
import threading
from logging import LoggerAdapter, getLogger
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING

from ....common.compat import ensure_binary
from ....common.serialize import json
from ....common.url import url_to_s3_info
from .. import BaseAdapter, CaseInsensitiveDict, Response

if TYPE_CHECKING:
    from .. import PreparedRequest

log = getLogger(__name__)
stderrlog = LoggerAdapter(getLogger("conda.stderrlog"), extra=dict(terminator="\n"))

_s3_client = None
_transfer_config = None
_client_lock = threading.Lock()


def _get_s3_client_and_config():
    """Get shared S3 client and TransferConfig with CRT support."""
    global _s3_client, _transfer_config
    if _s3_client is None:
        with _client_lock:
            if _s3_client is None:
                from boto3.s3.transfer import TransferConfig
                from boto3.session import Session

                from ....base.context import context

                _s3_client = Session().client("s3")
                # Use 'crt' to enable CRT acceleration when awscrt is installed
                # Falls back to classic transfer if CRT unavailable
                _transfer_config = TransferConfig(
                    max_concurrency=context.s3_max_concurrency,
                    multipart_threshold=context.s3_multipart_chunksize,
                    multipart_chunksize=context.s3_multipart_chunksize,
                    preferred_transfer_client="crt",
                )
    return _s3_client, _transfer_config


def reset_transfer_manager():
    """Reset singleton for testing or credential refresh."""
    global _s3_client, _transfer_config
    with _client_lock:
        _s3_client = None
        _transfer_config = None


class S3Adapter(BaseAdapter):
    def send(
        self,
        request: PreparedRequest,
        stream: bool = False,
        timeout: None | float | tuple[float, float] | tuple[float, None] = None,
        verify: bool | str = True,
        cert: None | bytes | str | tuple[bytes | str, bytes | str] = None,
        proxies: dict[str, str] | None = None,
    ) -> Response:
        resp = Response()
        resp.status_code = 200
        resp.url = request.url

        try:
            return self._send_boto3(resp, request)
        except ImportError:
            stderrlog.info(
                "\nError: boto3 is required for S3 channels. "
                "Please install with `conda install boto3`\n"
                "Make sure to run `conda deactivate` if you "
                "are in a conda environment.\n"
            )
            resp.status_code = 404
            return resp

    def close(self):
        pass

    def _send_boto3(self, resp: Response, request: PreparedRequest) -> Response:
        from botocore.exceptions import BotoCoreError, ClientError

        bucket_name, key_string = url_to_s3_info(request.url)
        key = key_string[1:]  # strip leading /

        client, config = _get_s3_client_and_config()

        try:
            head = client.head_object(Bucket=bucket_name, Key=key)

            fd, tmp_path = tempfile.mkstemp()
            os.close(fd)

            try:
                # Use client.download_file() to enable CRT acceleration
                client.download_file(bucket_name, key, tmp_path, Config=config)

                fh = SpooledTemporaryFile()
                with open(tmp_path, "rb") as f:
                    fh.write(f.read())
                fh.seek(0)
            finally:
                os.unlink(tmp_path)

            resp.headers = CaseInsensitiveDict(
                {
                    "Content-Type": head.get("ContentType", "text/plain"),
                    "Content-Length": str(head["ContentLength"]),
                    "Last-Modified": head["LastModified"].strftime(
                        "%a, %d %b %Y %H:%M:%S GMT"
                    ),
                }
            )
            resp.raw = fh
            resp.close = fh.close

        except (BotoCoreError, ClientError) as e:
            resp.status_code = 404
            message = {
                "error": "error downloading file from s3",
                "path": request.url,
                "exception": repr(e),
            }
            fh = SpooledTemporaryFile()
            fh.write(ensure_binary(json.dumps(message)))
            fh.seek(0)
            resp.raw = fh
            resp.close = fh.close

        return resp
