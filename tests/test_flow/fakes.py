import io
import logging
import os
from contextlib import contextmanager
from functools import partial
from glob import glob
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from bionic import gcs
from bionic.aip import client as aip_client
from bionic.aip.future import Future as AipFuture
from bionic.aip.main import _run as run_aip
from bionic.aip.task import Task as AipTask


class FakeAipFuture(AipFuture):
    """
    A mock implementation of Future that finished successfully.
    """

    def __init__(self, project_name: str, job_id: str, output: str):
        self.project_name = project_name
        self.job_id = job_id
        self.output = output

    def _get_state_and_error(self):
        from bionic.aip.future import State

        return State.SUCCEEDED, ""


class FakeAipTask(AipTask):
    """
    A mock implementation of Task that runs the job locally using a
    subprocess instead of running it on AIP.
    """

    def submit(self) -> AipFuture:
        self._stage()
        spec = self._ai_platform_job_spec()

        logging.info(f"Submitting test {self.config.project_name}: {self}")
        import subprocess

        subprocess.check_call(spec["trainingInput"]["args"])
        return FakeAipFuture(self.config.project_name, self.job_id(), self.output_uri())


class FakeAipExecutor:
    """
    A mock version of AipExecutor to submit FakeTasks that uses
    subprocess to execute the tasks instead of using AIP. Useful for
    running tests locally without using AIP.
    """

    def __init__(self, aip_config):
        self._aip_config = aip_config

    def submit(self, task_config, fn, *args, **kwargs):
        return FakeAipTask(
            name="a" + str(uuid4()).replace("-", ""),
            config=self._aip_config,
            task_config=task_config,
            function=partial(fn, *args, **kwargs),
        ).submit()


class FakeBlob:
    def __init__(self, bucket, name, blobs):
        assert not name.startswith("/")
        self.bucket = bucket
        self.name = name
        self.blobs = blobs

    def upload_from_string(self, data):
        self.blobs[self._url] = data

    def download_as_string(self):
        return self.blobs[self._url]

    def upload_from_filename(self, filename):
        self.upload_from_string(open(filename, "r").read())

    def download_to_filename(self, filename):
        open(filename, "w").write(self.download_as_string())

    def exists(self):
        return self._url in self.blobs

    def delete(self):
        del self.blobs[self._url]

    @property
    def _url(self):
        return f"gs://{self.bucket}/{self.name}"


class FakeBucket:
    def __init__(self, name, blobs):
        self.name = name
        self.blobs = blobs

    def blob(self, blob_name):
        return FakeBlob(self.name, blob_name, self.blobs)

    def get_blob(self, blob_name):
        return self.blob(blob_name)

    def list_blobs(self, prefix):
        assert not prefix.startswith("gs://")
        return [
            FakeBlob(self.name, k.replace(f"gs://{self.name}/", "", 1), self.blobs)
            for (k, v) in self.blobs.items()
            if k.startswith(f"gs://{self.name}/{prefix}")
        ]


class FakeGCS:
    def __init__(self):
        self.blobs = {}

    def get_bucket(self, bucket):
        return FakeBucket(bucket, self.blobs)

    def wipe_path(self, url):
        urls_to_remove = [k for k in self.blobs.keys() if k.startswith(url)]
        for k in urls_to_remove:
            del self.blobs[k]

    # Allows the blob to be used like a file object.
    # Used for mocking out blocks.filesystem.GCSNativeFileSystem.
    @contextmanager
    def open_blob(self, path, mode):
        f = io.BytesIO(self.blobs.get(path, b""))
        yield f
        self.blobs[path] = f.getvalue()

    def block_pickle(self, result, path):
        self.blobs[path] = result

    def block_unpickle(self, path):
        return self.blobs[path]

    def gsutil_cp(self, src_url, dst_url):
        """
        Emulate gsutil with the cp command.
        """
        if src_url.startswith("gs://"):
            assert dst_url.startswith("/")
            if src_url in self.blobs:
                assert not dst_url.endswith("/")
                open(dst_url, "wb").write(self.blobs[src_url])
            else:
                # When copying from a bucket directory to a file path, the
                # directory is copied over. For example, the following:
                #
                #   gsutil cp -r gs://my-bucket/data dir
                #
                # results in files with names like dir/data/a/b/c.

                last_component = src_url.rsplit("/", 1)[1]
                directory = Path(dst_url, last_component)
                directory.mkdir(parents=True, exist_ok=True)
                for url, data in self.blobs.items():
                    if not url.startswith(src_url):
                        continue
                    filename = url.rsplit("/", 1)[1]
                    open(Path(directory, filename), "wb").write(data)
        else:
            assert src_url.startswith("/")
            assert dst_url.startswith("gs://")
            if os.path.isfile(src_url):
                data = open(src_url, "rb").read()
                if dst_url.endswith("/"):
                    filename = os.path.basename(src_url)
                    self.blobs[f"{dst_url}{filename}"] = data
                else:
                    self.blobs[dst_url] = data
            else:
                for filename in glob(f"{src_url}/**"):
                    data = open(filename, "rb").read()
                    self.blobs[filename.replace(src_url, dst_url, 1)] = data


@contextmanager
def run_in_fake_gcp(fake_gcs: FakeGCS):
    """
    Use fake GCP by mocking out GCS and AIP.
    """

    mock_gcs_client = Mock()
    mock_gcs_client.get_bucket = FakeBucket

    def create_aip_job(body, parent):
        run_aip(body["trainingInput"]["args"][3])
        return Mock()

    mock_aip_client = Mock()
    mock_aip_client.projects().jobs().create = create_aip_job
    mock_aip_client.projects().jobs().get().execute.return_value = {
        "state": "SUCCEEDED"
    }

    with patch("blocks.pickle") as mock_pickle, patch(
        "blocks.unpickle"
    ) as mock_unpickle, patch(
        "blocks.filesystem.GCSNativeFileSystem"
    ) as mock_gcs_fs, patch(
        "bionic.gcs._gsutil_cp"
    ) as mock_gsutil_cp:
        mock_gcs_fs().open = fake_gcs.open_blob
        mock_pickle.side_effect = fake_gcs.block_pickle
        mock_unpickle.side_effect = fake_gcs.block_unpickle
        mock_gsutil_cp.side_effect = fake_gcs.gsutil_cp

        cached_gcs_client = gcs._cached_gcs_client
        cached_aip_client = aip_client._cached_aip_client
        try:
            gcs._cached_gcs_client = fake_gcs
            aip_client._cached_aip_client = mock_aip_client
            yield
        finally:
            gcs._cached_gcs_client = cached_gcs_client
            aip_client._cached_aip_client = cached_aip_client