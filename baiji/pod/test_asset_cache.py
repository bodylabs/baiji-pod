import unittest
import os
import mock
from baiji import s3
from bodylabs.util.test import BackupEnvMixin

class TestSCBase(unittest.TestCase):
    def setUp(self):
        from baiji.pod.static import AssetCache
        self.sc = AssetCache.create_default()

class TestSCExceptions(TestSCBase):
    def test_exceptions_interchangable_with_s3(self):
        from baiji.pod.static import AssetCache
        with self.assertRaises(s3.KeyNotFound):
            self.sc('s3://bodylabs-test/there/is/nothing/here/without.a.doubt')
        with self.assertRaises(AssetCache.KeyNotFound):
            self.sc('s3://bodylabs-test/there/is/nothing/here/without.a.doubt')

class TestSC(BackupEnvMixin, TestSCBase):
    @staticmethod
    def get_test_file_path():
        return os.path.abspath(os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'README.md'))

    def setUp(self):
        import tempfile
        import uuid
        from baiji.util.testing import create_random_temporary_file

        super(TestSC, self).setUp()

        self.sc.verbose = False

        self.cache_dir = tempfile.mkdtemp('BODYLABS_TEST_STATIC_CACHE_DIR')
        self.bucket = 'bodylabs-test'

        self.backup_env('STATIC_CACHE_DIR', 'STATIC_CACHE_DEFAULT_BUCKET', 'STATIC_CACHE_TIMEOUT')
        os.environ['STATIC_CACHE_DIR'] = self.cache_dir
        os.environ['STATIC_CACHE_DEFAULT_BUCKET'] = self.bucket
        os.environ['STATIC_CACHE_TIMEOUT'] = '1'

        self.filename = 'test_sc/{}/test_sample.txt'.format(uuid.uuid4())
        self.local_file = os.path.join(self.cache_dir, self.bucket, self.filename)
        self.timestamp_file = os.path.join(
            self.cache_dir, '.timestamps', self.bucket, self.filename)
        self.remote_file = 's3://{}/{}'.format(self.bucket, self.filename)

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
        self.assertEqual(self.sc(self.filename), self.local_file)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))

    def test_doesnt_check_before_timeout(self):
        self.sc(self.filename)
        with mock.patch('baiji.s3.cp') as mock_cp:
            mock_cp.return_value = True
            self.sc(self.filename)
            assert not mock_cp.called, 'File downloaded before timeout'

    def test_does_check_after_timeout(self):
        import time

        self.sc(self.filename)

        s3.cp(self.get_test_file_path(), self.remote_file, force=True)
        time.sleep(2)

        with mock.patch('baiji.s3.cp') as mock_cp:
            mock_cp.return_value = True
            self.sc(self.filename)
            mock_cp.assert_called_with(
                self.remote_file, self.local_file,
                progress=False, force=True, validate=True)

    def test_that_invalidating_nonexistent_file_succeeds(self):
        import uuid
        nonexistent_path = str(uuid.uuid4()) + '.txt'
        self.sc.invalidate(nonexistent_path)

    def test_that_invalidating_single_file_removes_its_timestamp(self):
        self.sc(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))

        self.sc.invalidate(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertFalse(os.path.exists(self.timestamp_file))

    def test_that_invalidating_tree_removes_child_timestamps(self):
        import uuid
        path = 'test_sc/{}'.format(uuid.uuid4())
        filenames = ['{}/test_sample_{}.txt'.format(path, i) for i in range(3)]
        for filename in filenames:
            remote_file = 's3://{}/{}'.format(self.bucket, filename)
            s3.cp(self.temp_file, remote_file)
            self.sc(filename)
            s3.rm(remote_file)

        for filename in filenames:
            timestamp_file = os.path.join(
                self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertTrue(os.path.exists(timestamp_file))

        self.sc.invalidate(path)
        for filename in filenames:
            timestamp_file = os.path.join(
                self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertFalse(os.path.exists(timestamp_file))
