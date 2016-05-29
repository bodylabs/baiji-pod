import unittest
import os
import mock
from baiji import s3


class CreateDefaultAssetCacheMixin(object):
    def setUp(self):
        from baiji.pod import AssetCache
        super(CreateDefaultAssetCacheMixin, self).setUp()
        self.cache = AssetCache.create_default()


class CreateTestAssetCacheMixin(object):
    def setUp(self):
        import tempfile
        from baiji.pod import AssetCache
        from baiji.pod.config import Config

        super(CreateTestAssetCacheMixin, self).setUp()

        self.cache_dir = tempfile.mkdtemp('BAIJI_POD_TEST_STATIC_CACHE')
        # Force the code to be robust to a missing cache directory. Catches a
        # failure when writing `missing_assets.yaml` without the directory
        # existing first.
        os.rmdir(self.cache_dir)

        self.bucket = 'baiji-pod-test'

        config = Config()
        config.CACHE_DIR = self.cache_dir
        config.DEFAULT_BUCKET = self.bucket
        config.TIMEOUT = 1
        config.VERBOSE = False
        self.cache = AssetCache(config)

    def tearDown(self):
        import shutil
        super(CreateTestAssetCacheMixin, self).tearDown()
        shutil.rmtree(self.cache_dir, ignore_errors=True)


class TestAssetCacheExceptions(CreateDefaultAssetCacheMixin, unittest.TestCase):
    def test_exceptions_interchangable_with_s3(self):
        from baiji.pod import AssetCache

        with self.assertRaises(s3.KeyNotFound):
            self.cache('s3://baiji-pod-test/there/is/nothing/here/without.a.doubt')
        with self.assertRaises(AssetCache.KeyNotFound):
            self.cache('s3://baiji-pod-test/there/is/nothing/here/without.a.doubt')


class TestMissingAssets(CreateTestAssetCacheMixin, unittest.TestCase):
    @mock.patch('baiji.config.credentials', new_callable=mock.PropertyMock)
    def test_missing_assets(self, mock_credentials):
        from baiji.exceptions import AWSCredentialsMissing
        from baiji.pod.util import yaml

        # See
        # http://www.voidspace.org.uk/python/mock/examples.html#raising-exceptions-on-attribute-access
        type(mock_credentials).key = mock.PropertyMock(side_effect=AWSCredentialsMissing)

        nonexistent_path = 's3://baiji-pod-foo/there/is/nothing/here/without.a.doubt'

        with self.assertRaises(AWSCredentialsMissing):
            # Sanity check. ^^^
            self.cache(nonexistent_path)

        missing_assets_path = os.path.join(self.cache_dir, 'missing_assets.yaml')
        missing_assets = yaml.load(missing_assets_path)

        self.assertEqual(missing_assets, [nonexistent_path])

class TestAssetCache(CreateTestAssetCacheMixin, unittest.TestCase):
    @staticmethod
    def get_test_file_path():
        return os.path.abspath(os.path.join(
            os.path.dirname(__file__),
            '..',
            '..',
            'README.md'))

    def setUp(self):
        import uuid
        from baiji.util.testing import create_random_temporary_file

        super(TestAssetCache, self).setUp()

        self.filename = 'test_sc/{}/test_sample.txt'.format(uuid.uuid4())
        self.local_file = os.path.join(self.cache_dir, self.bucket, self.filename)
        self.timestamp_file = os.path.join(
            self.cache_dir, '.timestamps', self.bucket, self.filename)
        self.remote_file = 's3://{}/{}'.format(self.bucket, self.filename)

        self.temp_file = create_random_temporary_file()
        s3.cp(self.temp_file, self.remote_file)

    def tearDown(self):
        super(TestAssetCache, self).setUp()
        os.remove(self.temp_file)
        s3.rm(self.remote_file)

    def test_basic_functionality(self):
        self.assertFalse(os.path.exists(self.local_file))
        self.assertFalse(os.path.exists(self.timestamp_file))
        self.assertEqual(self.cache(self.filename), self.local_file)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))

    def test_doesnt_check_before_timeout(self):
        self.cache(self.filename)
        with mock.patch('baiji.s3.cp') as mock_cp:
            mock_cp.return_value = True
            self.cache(self.filename)
            assert not mock_cp.called, 'File downloaded before timeout'

    def test_does_check_after_timeout(self):
        import time

        self.cache(self.filename)

        s3.cp(self.get_test_file_path(), self.remote_file, force=True)
        time.sleep(2)

        with mock.patch('baiji.s3.cp') as mock_cp:
            mock_cp.return_value = True
            self.cache(self.filename)
            mock_cp.assert_called_with(
                self.remote_file, self.local_file,
                progress=False, force=True, validate=True)

    def test_that_invalidating_nonexistent_file_succeeds(self):
        import uuid
        nonexistent_path = str(uuid.uuid4()) + '.txt'
        self.cache.invalidate(nonexistent_path)

    def test_that_invalidating_single_file_removes_its_timestamp(self):
        self.cache(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertTrue(os.path.exists(self.timestamp_file))

        self.cache.invalidate(self.filename)
        self.assertTrue(os.path.exists(self.local_file))
        self.assertFalse(os.path.exists(self.timestamp_file))

    def test_that_invalidating_tree_removes_child_timestamps(self):
        import uuid
        path = 'test_sc/{}'.format(uuid.uuid4())
        filenames = ['{}/test_sample_{}.txt'.format(path, i) for i in range(3)]
        for filename in filenames:
            remote_file = 's3://{}/{}'.format(self.bucket, filename)
            s3.cp(self.temp_file, remote_file)
            self.cache(filename)
            s3.rm(remote_file)

        for filename in filenames:
            timestamp_file = os.path.join(
                self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertTrue(os.path.exists(timestamp_file))
            cache_file = os.path.join(
                self.cache_dir, self.bucket, filename)
            self.assertTrue(os.path.exists(cache_file))

        self.cache.invalidate(path)
        for filename in filenames:
            timestamp_file = os.path.join(
                self.cache_dir, '.timestamps', self.bucket, filename)
            self.assertFalse(os.path.exists(timestamp_file))
            cache_file = os.path.join(
                self.cache_dir, self.bucket, filename)
            self.assertTrue(os.path.exists(cache_file))


class TestCacheFile(CreateDefaultAssetCacheMixin, unittest.TestCase):
    def test_cachefile_parses_s3_path_correctly(self):
        from baiji.pod.asset_cache import CacheFile
        cf = CacheFile(self.cache, 's3://BuKeT/foo/bar.baz')
        self.assertEqual(cf.path, '/foo/bar.baz')
        self.assertEqual(cf.bucket, 'BuKeT')
        self.assertEqual(cf.local, os.path.join(self.cache.config.cache_dir, 'BuKeT', 'foo/bar.baz'))
        self.assertEqual(cf.remote, 's3://BuKeT/foo/bar.baz')
        self.assertEqual(cf.timestamp_file, os.path.join(self.cache.config.cache_dir, '.timestamps', 'BuKeT', 'foo/bar.baz'))

    def test_cachefile_parses_recursive_cached_calls_correctly(self):
        from baiji.pod.asset_cache import CacheFile
        with mock.patch('baiji.s3.cp') as mock_cp:
            mock_cp.return_value = True
            with mock.patch('baiji.s3.exists') as mock_exists:
                mock_exists.return_value = True
                local_path = self.cache('s3://BuKeT/foo/bar.baz')
        self.assertEqual(local_path, os.path.join(self.cache.config.cache_dir, 'BuKeT', 'foo/bar.baz'))
        cf = CacheFile(self.cache, local_path)
        self.assertEqual(cf.path, '/foo/bar.baz')
        self.assertEqual(cf.bucket, 'BuKeT')
        self.assertEqual(cf.local, os.path.join(self.cache.config.cache_dir, 'BuKeT', 'foo/bar.baz'))
        self.assertEqual(cf.remote, 's3://BuKeT/foo/bar.baz')
        self.assertEqual(cf.timestamp_file, os.path.join(self.cache.config.cache_dir, '.timestamps', 'BuKeT', 'foo/bar.baz'))

    def test_cachefile_parses_remote_path_with_no_bucket_correctly(self):
        from baiji.pod.asset_cache import CacheFile
        self.cache.config.DEFAULT_BUCKET = 'BuKeT'
        cf = CacheFile(self.cache, '/foo/bar.baz')
        self.assertEqual(cf.path, '/foo/bar.baz')
        self.assertEqual(cf.bucket, 'BuKeT')
        self.assertEqual(cf.local, os.path.join(self.cache.config.cache_dir, 'BuKeT', 'foo/bar.baz'))
        self.assertEqual(cf.remote, 's3://BuKeT/foo/bar.baz')
        self.assertEqual(cf.timestamp_file, os.path.join(self.cache.config.cache_dir, '.timestamps', 'BuKeT', 'foo/bar.baz'))

    def test_cachefile_parses_remote_path_with_explicit_bucket_correctly(self):
        from baiji.pod.asset_cache import CacheFile
        cf = CacheFile(self.cache, '/foo/bar.baz', bucket='BuKeT')
        self.assertEqual(cf.path, '/foo/bar.baz')
        self.assertEqual(cf.bucket, 'BuKeT')
        self.assertEqual(cf.local, os.path.join(self.cache.config.cache_dir, 'BuKeT', 'foo/bar.baz'))
        self.assertEqual(cf.remote, 's3://BuKeT/foo/bar.baz')
        self.assertEqual(cf.timestamp_file, os.path.join(self.cache.config.cache_dir, '.timestamps', 'BuKeT', 'foo/bar.baz'))

    def test_that_age_of_nonexistent_file_is_forever(self):
        import uuid
        from baiji.pod.asset_cache import CacheFile
        nonexistent_path = str(uuid.uuid4()) + '.txt'
        cf = CacheFile(self.cache, nonexistent_path, bucket='BuKeT')
        self.assertEqual(cf.age, float('inf'))

    def test_that_file_operations_on_nonexistent_file_are_safe(self):
        import uuid
        from baiji.pod.asset_cache import CacheFile
        nonexistent_path = str(uuid.uuid4()) + '.txt'
        cf = CacheFile(self.cache, nonexistent_path, bucket='BuKeT')
        self.assertIsNone(cf.timestamp)
        self.assertIsNone(cf.size)
        self.assertTrue(cf.is_outdated)
        self.assertFalse(cf.is_cached)
        cf.invalidate() # should not raise
        cf.remove_cached() # should not raise
