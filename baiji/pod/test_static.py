import unittest
import os
import uuid
import mock
from baiji import s3
from baiji.pod.static import sc
from bodylabs.util.test import BackupEnvMixin


class TestSCExceptions(unittest.TestCase):
    def test_exceptions_interchangable_with_s3(self):
        with self.assertRaises(s3.KeyNotFound):
            sc('s3://bodylabs-test/there/is/nothing/here/without.a.doubt')
        with self.assertRaises(sc.KeyNotFound):
            sc('s3://bodylabs-test/there/is/nothing/here/without.a.doubt')

class TestSC(BackupEnvMixin, unittest.TestCase):
    @staticmethod
    def get_test_file_path():
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'README.md'))

    def setUp(self):
        import tempfile
        from baiji.util.testing import create_random_temporary_file

        sc.verbose = False
        self.cache_dir = tempfile.mkdtemp('BODYLABS_TEST_STATIC_CACHE_DIR')
        self.bucket = 'bodylabs-test'

        self.backup_env('STATIC_CACHE_DIR', 'STATIC_CACHE_DEFAULT_BUCKET', 'STATIC_CACHE_TIMEOUT')
        os.environ['STATIC_CACHE_DIR'] = self.cache_dir
        os.environ['STATIC_CACHE_DEFAULT_BUCKET'] = self.bucket
        os.environ['STATIC_CACHE_TIMEOUT'] = '1'

        self.filename = "test_sc/%s/test_sample.txt" % uuid.uuid4()
        self.local_file = os.path.join(self.cache_dir, self.bucket, self.filename)
        self.timestamp_file = os.path.join(self.cache_dir, '.timestamps', self.bucket, self.filename)
        self.remote_file = "s3://%s/%s" % (self.bucket, self.filename)

        self.temp_file = create_random_temporary_file()
        s3.cp(self.temp_file, self.remote_file)

    def tearDown(self):
        import shutil
        self.restore_env('STATIC_CACHE_DIR', 'STATIC_CACHE_DEFAULT_BUCKET', 'STATIC_CACHE_TIMEOUT')
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        os.remove(self.temp_file)
        s3.rm(self.remote_file)

    def test_basic_functionality(self):
        self.assertFalse(os.path.exists(self.local_file))
        self.assertFalse(os.path.exists(self.timestamp_file))
        self.assertEqual(sc(self.filename), self.local_file)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))

    def test_doesnt_check_before_timeout(self):
        sc(self.filename)
        with mock.patch('bodylabs.cloud.s3.cp') as mock_cp:
            mock_cp.return_value = True
            sc(self.filename)
            assert not mock_cp.called, "File downloaded before timeout"

    def test_does_check_after_timeout(self):
        import time
        sc(self.filename)
        s3.cp(this.get_test_file_path(), self.remote_file, force=True)
        time.sleep(2)
        with mock.patch('bodylabs.cloud.s3.cp') as mock_cp:
            mock_cp.return_value = True
            sc(self.filename)
            mock_cp.assert_called_with(self.remote_file, self.local_file, progress=False, force=True, validate=True)

    def test_that_invalidating_nonexistent_file_succeeds(self):
        nonexistent_path = str(uuid.uuid4()) + '.txt'
        sc.invalidate(nonexistent_path)

    def test_that_invalidating_single_file_removes_its_timestamp(self):
        sc(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))
        sc.invalidate(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertFalse(os.path.exists(self.timestamp_file))

    def test_that_invalidating_tree_removes_child_timestamps(self):
        path = "test_sc/%s" % uuid.uuid4()
        filenames = ["%s/test_sample_%s.txt" % (path, i) for i in range(3)]
        for filename in filenames:
            remote_file = "s3://%s/%s" % (self.bucket, filename)
            s3.cp(self.temp_file, remote_file)
            sc(filename)
            s3.rm(remote_file)
        for filename in filenames:
            timestamp_file = os.path.join(self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertTrue(os.path.exists(timestamp_file))
        sc.invalidate(path)
        for filename in filenames:
            timestamp_file = os.path.join(self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertFalse(os.path.exists(timestamp_file))
