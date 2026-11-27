from __future__ import annotations

import hashlib
import io
import os
import uuid

import pytest

from wes.blobstore import LocalBlobStore, make_blobstore, object_key, spool


def roundtrip(store, tenant="t1"):
    data = os.urandom(3 * 1024 * 1024 + 7)  # spans several read chunks
    sp = spool(io.BytesIO(data))
    try:
        assert sp.sha256 == hashlib.sha256(data).hexdigest() and sp.size == len(data)
        uri = store.upload(sp, tenant)
        assert uri == store.uri_for(tenant, sp.sha256)
        assert store.upload(sp, tenant) == uri  # idempotent
    finally:
        sp.cleanup()
    assert not sp.path.exists()
    assert store.exists(uri)
    with store.open(uri) as f:
        assert f.read() == data
    store.delete(uri)
    assert not store.exists(uri)


def test_local_store_roundtrip_and_layout(tmp_path):
    store = LocalBlobStore(tmp_path)
    roundtrip(store)
    sp = spool(b"abc")
    uri = store.upload(sp, "t1")
    assert uri.endswith(object_key("t1", sp.sha256))
    assert "/tenants/t1/sha256/ba/78/" in uri


def test_local_store_refuses_paths_outside_its_root(tmp_path):
    store = LocalBlobStore(tmp_path / "root")
    with pytest.raises(ValueError):
        store.open(f"file://{tmp_path}/elsewhere")
    with pytest.raises(ValueError):
        store.open(f"file://{tmp_path}/root/../secret")
    with pytest.raises(ValueError):
        object_key("../../etc", "0" * 64)


def test_make_blobstore_parses_urls(tmp_path):
    assert isinstance(make_blobstore(f"file://{tmp_path}"), LocalBlobStore)
    assert isinstance(make_blobstore(str(tmp_path)), LocalBlobStore)
    with pytest.raises(ValueError):
        make_blobstore("s3://bucket/with/prefix")


@pytest.mark.skipif(not os.environ.get("WES_TEST_S3_ENDPOINT"),
                    reason="set WES_TEST_S3_ENDPOINT (e.g. a MinIO) to test the S3 store")
def test_s3_store_roundtrip(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    endpoint = os.environ["WES_TEST_S3_ENDPOINT"]
    bucket = f"wes-test-{uuid.uuid4().hex[:8]}"
    store = make_blobstore(f"s3://{bucket}", s3_endpoint=endpoint, s3_region="us-east-1",
                           s3_create_bucket=True)
    store.ensure_bucket()  # idempotent
    try:
        roundtrip(store)
    finally:
        for obj in store.client.list_objects_v2(Bucket=bucket).get("Contents", []):
            store.client.delete_object(Bucket=bucket, Key=obj["Key"])
        store.client.delete_bucket(Bucket=bucket)
