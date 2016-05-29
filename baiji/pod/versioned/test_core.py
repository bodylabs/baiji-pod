import unittest
import mock
from scratch_dir import ScratchDirMixin


class TestVC(ScratchDirMixin, unittest.TestCase):
    def setUp(self):
        import os
        from baiji.pod.util import json

        super(TestVC, self).setUp()

        # Some random local file
        self.local_json_file = os.path.join(self.scratch_dir, 'local_json_file.json')
        json.dump({'a': 42}, self.local_json_file)

        # Mock manifest
        self.manifest = {
            '/foo/bar.csv': '1.2.5',
            '/foo/bar.json': '0.1.6',
            '/foo/semver.ftw': '0.1.6-foo',
            '/test_local.json': self.local_json_file,
            '/test_error.json': 'THIS IS NOT A VALID PATH',
            # this would be a different bucket in practice, but this lets the
            # mock work easily:
            '/test_remote.json': 's3://baiji-pod-mock-versioned-assets/foo.json',
        }
        self.manifest_file = os.path.join(self.scratch_dir, 'manifest.json')
        json.dump(self.manifest, self.manifest_file)

        self.bucket_contents = [
            u'/foo/bar.1.2.3.csv',
            u'/foo/bar.1.2.5.csv',
            u'/foo/bar.0.1.6.json',
        ]

    def mock_vc(self):
        from baiji.pod import VersionedCache
        def cache(s, bucket=None, force_check=False, verbose=None, stacklevel=1):
            '''
            A mock of AssetCache that doesn't do anything but...
            '''
            _ = bucket, force_check, verbose, stacklevel  # For pylint.
            return s.replace('s3://baiji-pod-mock-versioned-assets', '/local')

        return VersionedCache(
            cache=cache,
            manifest_path=self.manifest_file,
            bucket='baiji-pod-mock-versioned-assets')

    def real_vc(self):
        import os
        from baiji.pod import Config
        from baiji.pod import AssetCache
        from baiji.pod import VersionedCache

        bucket = 'baiji-test-versioned-assets'

        config = Config()
        config.CACHE_DIR = os.path.join(self.scratch_dir, 'test_vc_cache')
        config.IMMUTABLE_BUCKETS = [bucket]

        return VersionedCache(
            cache=AssetCache(config),
            manifest_path=self.manifest_file,
            bucket=bucket)

    @mock.patch('baiji.s3.exists')
    def test_semver_works(self, exists_mock):
        vc = self.mock_vc()

        exists_mock.return_value = True
        self.assertEqual(
            vc('/foo/semver.ftw'), '/local/foo/semver.0.1.6-foo.ftw')

    def test_missing_version(self):
        from baiji import s3

        vc = self.mock_vc()
        vc('/foo/bar.csv', version='1.2.3')

        with mock.patch.object(vc, 'cache', side_effect=s3.KeyNotFound()):
            error_msg = 'not cached for version 1.2.4'
            with self.assertRaisesRegexp(vc.KeyNotFound, error_msg):
                return vc('/foo/bar.csv', version='1.2.4')

    @mock.patch('baiji.s3.exists')
    def test_getting_versioned_items(self, exists_mock):
        vc = self.mock_vc()

        self.assertEqual(vc('/foo/bar.json'), '/local/foo/bar.0.1.6.json')
        self.assertEqual(vc('foo/bar.json'), '/local/foo/bar.0.1.6.json')
        self.assertEqual(vc('foo/bar.csv'), '/local/foo/bar.1.2.5.csv')

        with self.assertRaises(vc.KeyNotFound):
            vc('/foo')

        self.assertEqual(vc('test_local.json'), self.local_json_file)

        exists_mock.return_value = False
        with self.assertRaises(vc.KeyNotFound):
            vc('/test_error.json')

        exists_mock.return_value = True
        self.assertEqual(vc('test_remote.json'), '/local/foo.json')

    def test_sc_does_not_recheck(self):
        vc = self.real_vc()

        def touch_dst(src, dst, *args, **kwargs):
            import os
            from baiji.util.shutillib import mkdir_p
            _ = src, args, kwargs
            mkdir_p(os.path.dirname(dst))
            with open(dst, 'w'):
                pass
        with mock.patch('baiji.s3.cp', side_effect=touch_dst):
            vc('/foo/bar.csv')

        vc.cache.invalidate(vc.uri('/foo/bar.csv'))

        # We use etag here because not only should we not re-download
        # versioned files, we shouldn't even have to check if they're still
        # valid.
        with mock.patch('baiji.s3.etag') as mock_etag:
            mock_etag.return_value = True
            vc('/foo/bar.csv')
            self.assertFalse(mock_etag.called, 'sc tried to recheck a versioned file')

    def test_version_parsing(self):
        vc = self.real_vc()

        self.assertEqual(vc.extract_version('/dir/file.0.1.6.ext'), '0.1.6')
        self.assertEqual(vc.extract_version('/dots.in.filename.0.14.6.ext'), '0.14.6')
        self.assertEqual(vc.extract_version('/file_with_no_ext.10.1.6'), '10.1.6')
        self.assertEqual(vc.extract_version('/file_with_no_ext.0.1.65'), '0.1.65')

        with self.assertRaises(ValueError):
            vc.extract_version('/file_with_no_version.ext')
        with self.assertRaises(ValueError):
            vc.extract_version('/file_with_invalid_version.0.1.ext')
        with self.assertRaises(ValueError):
            vc.extract_version('/file_with_invalid_version.01.1.9.ext')

    @mock.patch('baiji.s3.ls')
    def test_current_and_next_version(self, mock_ls):
        vc = self.real_vc()
        mock_ls.return_value = self.bucket_contents

        self.assertEqual(vc.latest_available_version('/foo/bar.csv'), '1.2.5')
        self.assertEqual(vc.next_version_number('/foo/bar.csv'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '0'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '2'), '2.0.0')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '0.3'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1.1'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1.2'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1.3'), '1.3.0')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1.2.4'), '1.2.6')
        self.assertEqual(vc.next_version_number('/foo/bar.csv', '1.2.13'), '1.2.13')

    @mock.patch('baiji.s3.cp')
    @mock.patch('baiji.s3.ls')
    def test_new(self, mock_ls, mock_cp):
        vc = self.mock_vc()
        mock_ls.return_value = self.bucket_contents

        with self.assertRaises(ValueError):
            vc.add('/foo/bar.csv', self.local_json_file)

        vc.add('/new/foo.a', self.local_json_file)
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/new/foo.1.0.0.a',
            progress=False)

        vc.add('/new/foo.b', self.local_json_file, version='1.2.3')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/new/foo.1.2.3.b',
            progress=False)

        vc.add('/new/foo.c', self.local_json_file, version='1.2')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/new/foo.1.2.0.c',
            progress=False)

    @mock.patch('baiji.s3.cp')
    @mock.patch('baiji.s3.ls')
    def test_update(self, mock_ls, mock_cp):
        vc = self.mock_vc()
        mock_ls.return_value = self.bucket_contents

        self.assertEqual(vc.latest_available_version('/foo/bar.csv'), '1.2.5')

        with self.assertRaises(vc.KeyNotFound):
            vc.update('/new/foo.ext', self.local_json_file, version='1.1.1')
        with self.assertRaises(ValueError):
            vc.update('/foo/bar.csv', self.local_json_file, version='0.2.6')

        vc.update('/foo/bar.csv', self.local_json_file, version='1.2.42')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.1.2.42.csv',
            progress=False)

        vc.update(
            '/foo/bar.csv', self.local_json_file,
            version='1.2.42', major=True, min_version='3.1.1')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.1.2.42.csv',
            progress=False)

        vc.update(
            '/foo/bar.csv', self.local_json_file,
            major=True, min_version='1.2.42')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.2.0.0.csv',
            progress=False)

        vc.update(
            '/foo/bar.csv', self.local_json_file,
            major=True, min_version='3.1.1')
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.3.1.1.csv',
            progress=False)

        vc.update_major('/foo/bar.csv', self.local_json_file)
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.2.0.0.csv',
            progress=False)

        vc.update_minor('/foo/bar.csv', self.local_json_file)
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.1.3.0.csv',
            progress=False)

        vc.update_patch('/foo/bar.csv', self.local_json_file)
        mock_cp.assert_called_with(
            self.local_json_file,
            's3://baiji-pod-mock-versioned-assets/foo/bar.1.2.6.csv',
            progress=False)

    @mock.patch('baiji.pod.VersionedCache.update')
    @mock.patch('baiji.pod.VersionedCache.add')
    def test_add_or_update(self, mock_add, mock_update):
        vc = self.mock_vc()

        version = '1.2.42'
        vc.add_or_update('/foo/bar.csv', self.local_json_file, version=version)
        mock_update.assert_called_with(
            '/foo/bar.csv', self.local_json_file, version=version, major=False,
            minor=False, patch=False, min_version=None, verbose=False)

        vc.add_or_update(
            '/foo/bar.csv', self.local_json_file, version=version, major=True)
        mock_update.assert_called_with(
            '/foo/bar.csv', self.local_json_file, version=version, major=True,
            minor=False, patch=False, min_version=None, verbose=False)

        vc.add_or_update(
            '/foo/bar_new.csv', self.local_json_file, version=version)
        mock_add.assert_called_with(
            '/foo/bar_new.csv', self.local_json_file,
            version=version, verbose=False)
