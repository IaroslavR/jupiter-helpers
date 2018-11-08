import hashlib
import logging
import os
import shutil
from pathlib import Path

import boto3
import pytest
import pytest_localstack
import requests

# noinspection PyUnresolvedReferences
localstack = pytest_localstack.patch_fixture(
    scope="module",  # Use the same Localstack container for all tests in this module.
    services=["s3"],  # Limit to the AWS services you need.
    autouse=True,  # Automatically use this fixture in tests.
    region_name="ap-southeast-1",
    container_log_level=logging.NOTSET,
    pull_image=False,
)

from sagemaker_helpers import Storage

web_source = "https://www.dropbox.com/s/iw7fohd4bykm6ti/traffic_jam_1050x700.jpg?dl=1"
fname = "traffic_jam_1050x700.jpg"
fpath = os.path.join("assertions", fname)
with open(fpath, "rb") as f:
    fcontent = f.read()
expected_hash = "36fab743f782449ed474e487edf87118"
bucket_name = "sagemaker-iaro-test"
notebook_id = "sagemaker-helpers-test"


@pytest.fixture
def mock_download_from_url(monkeypatch):
    monkeypatch.setattr(Storage, "download_from_url", lambda *args, **kwargs: fcontent)


def get_file_hash(fname: str) -> str:
    return hashlib.md5(open(fname, "rb").read()).hexdigest()


def download_file(url: str, path: str) -> str:
    r = requests.get(url)
    with open(path, "wb") as f:
        f.write(r.content)
    return str(Path(fname).resolve())


fhash = get_file_hash(fpath)

with open(fpath, "rb") as f:
    fdata = f.read()


def teardown_module():
    obj = Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        init_local=False,
        init_bucket=False,
        get_role=False,
        download_now=False,
    )
    shutil.rmtree(obj.local_root_path)


def test_name_parsing():
    obj = Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        init_local=False,
        init_bucket=False,
        get_role=False,
        download_now=False,
    )
    assert obj.fname == fname


def test_web_source(mock_download_from_url, log):
    obj = Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        init_bucket=False,
        get_role=False,
    )
    assert log.has("file downloaded")
    assert expected_hash == get_file_hash(obj.local_full_name)


def test_skip_web_source(log, mock_download_from_url):
    Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        init_bucket=False,
        get_role=False,
    )
    assert log.has("file already exists, download skipped")


def test_wrong_bucket_name():
    with pytest.raises(ValueError):
        Storage(
            bucket_name="iaro-test",
            notebook_id=notebook_id,
            source=web_source,
            init_bucket=False,
            get_role=False,
        )


def test_upload_to_s3(mock_download_from_url, tmpdir, log):
    obj = Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        get_role=False,
        copy_to_s3=True,
    )
    tmpf = tmpdir.mkdir("tmp").join(fname)
    s3 = boto3.client("s3")
    s3.download_file(obj.bucket_name, obj.s3_key, str(tmpf))
    assert expected_hash == get_file_hash(tmpf)


def test_skip_upload_to_s3(mock_download_from_url, log):
    Storage(
        bucket_name=bucket_name,
        notebook_id=notebook_id,
        source=web_source,
        get_role=False,
        copy_to_s3=True,
    )
    assert log.has("key exists upload skipped")
