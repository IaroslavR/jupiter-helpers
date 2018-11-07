"""sagemaker storage helper"""
import os
import posixpath
from pathlib import Path
from urllib.parse import urlsplit, unquote

import attr
import boto3
import requests
import sagemaker
import structlog
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from sagemaker import get_execution_role

log = structlog.get_logger()


@attr.s(auto_attribs=True)
class Storage(object):
    bucket_name: str  # S3 root
    notebook_id: str  # unique prefix for notebook data identification and paths generation
    source: str  # source url
    local_root_path: str = "raw_data_store"  # filesystem root
    init_bucket: bool = True
    init_local: bool = True
    get_role: bool = True
    download_now: bool = True
    copy_to_s3: bool = False
    overwrite_existed: bool = False
    s3: BaseClient = None
    bucket: str = None
    role: str = None
    session: str = None

    def __attrs_post_init__(self):
        if "sagemaker" not in self.bucket_name:
            raise ValueError(
                "Bucket name must contains 'sagemaker' "
                "see https://docs.aws.amazon.com/sagemaker/latest/dg/gs-config-permissions.html "
                "for details"
            )
        if self.get_role:
            self.role = get_execution_role()
            log.debug(self.role)
            self.session = sagemaker.Session()
        if self.init_local:
            os.makedirs(self.local_path, exist_ok=True)  # create if not exists
        if self.init_bucket:
            self.s3 = boto3.client("s3")
            self.s3.create_bucket(Bucket=self.bucket_name)  # create if not exists
        if self.download_now:
            self.download_from_source()

    @property
    def fname(self):
        as_path = urlsplit(self.source).path
        basename = posixpath.basename(unquote(as_path))
        return basename

    @property
    def local_path(self):
        abs_path = str(Path(self.local_root_path).resolve())
        return os.path.join(abs_path, self.notebook_id)

    @property
    def local_full_name(self):
        return os.path.join(self.local_path, self.fname)

    @property
    def s3_key(self):
        """Return S3 bucket key according to file name"""
        return f"{self.notebook_id}/{self.fname}"

    @property
    def s3_key_exists(self) -> bool:
        try:
            self.s3.Object(self.bucket_name, self.s3_key).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise
        else:
            return True

    @property
    def local_file_exists(self) -> bool:
        return os.path.isfile(self.local_full_name)

    def upload_to_s3(self):
        if self.s3_key_exists:
            if self.overwrite_existed:
                self.s3.upload_file(self.local_full_name, self.bucket_name, self.s3_key)
                log.info("key overwritten", key=self.s3_key)
            else:
                log.debug("key exists upload skipped", key=self.s3_key)
        else:
            self.s3.upload_file(self.local_full_name, self.bucket_name, self.s3_key)
            log.info("key uploaded", key=self.s3_key)

    def download_from_s3(self, fname):
        raise NotImplementedError

    def upload_to_efs(self):
        raise NotImplementedError

    def download_from_efs(self, fname):
        raise NotImplementedError

    def download_from_source(self) -> str:
        target = self.local_full_name
        if not self.overwrite_existed and os.path.isfile(target):
            log.debug("file already exists, download skipped", path=target)
        else:
            r = requests.get(self.source)
            with open(target, "wb") as f:
                f.write(r.content)
        if self.copy_to_s3 and self.overwrite_existed:
            raise NotImplementedError
        return target
