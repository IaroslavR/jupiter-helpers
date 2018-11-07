import hashlib
import os
import shutil
from pathlib import Path

import pytest
import requests

from sagemaker_helpers import Storage

web_source = "https://www.dropbox.com/s/iw7fohd4bykm6ti/traffic_jam_1050x700.jpg?dl=1"
fname = "traffic_jam_1050x700.jpg"
fpath = os.path.join("assertions", fname)
expected_hash = "36fab743f782449ed474e487edf87118"

import pytest_localstack

pytest_localstack.patch_fixture(
    services=["s3"],  # Limit to the AWS services you need.
    scope="module",  # Use the same Localstack container for all tests in this module.
    autouse=True,  # Automatically use this fixture in tests.
)


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
        bucket_name="sagemaker-iaro-test",
        notebook_id="test-jupiter-helpers",
        source=web_source,
        init_local=False,
        init_bucket=False,
        get_role=False,
        download_now=False,
    )
    shutil.rmtree(obj.local_root_path)


def test_name():
    obj = Storage(
        bucket_name="sagemaker-iaro-test",
        notebook_id="test-jupiter-helpers",
        source=web_source,
        init_local=False,
        init_bucket=False,
        get_role=False,
        download_now=False,
    )
    assert obj.fname == fname


def test_web_source(requests_mock):
    requests_mock.get(web_source, content=fdata)
    obj = Storage(
        bucket_name="sagemaker-iaro-test",
        notebook_id="test-jupiter-helpers",
        source=web_source,
        init_bucket=False,
        get_role=False,
    )
    assert expected_hash == get_file_hash(obj.local_full_name)


def test_skip_downloaded_source(log, requests_mock):
    requests_mock.get(web_source, content=fdata)
    Storage(
        bucket_name="sagemaker-iaro-test",
        notebook_id="test-jupiter-helpers",
        source=web_source,
        init_bucket=False,
        get_role=False,
    )
    assert log.has("file already exists, download skipped")


def test_wrong_bucket_name(requests_mock):
    requests_mock.get(web_source, content=fdata)
    with pytest.raises(ValueError):
        Storage(
            bucket_name="iaro-test",
            notebook_id="test-jupiter-helpers",
            source=web_source,
            init_bucket=False,
            get_role=False,
        )


def _test_upload_to_s3(requests_mock):
    requests_mock.get(web_source, content=fdata)
    obj = Storage(
        bucket_name="sagemaker-iaro-test",
        notebook_id="test-jupiter-helpers",
        source=web_source,
        get_role=False,
    )
