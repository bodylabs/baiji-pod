from __future__ import print_function
import unittest
import mock
from baiji.pod.test_asset_cache import CreateDefaultAssetCacheMixin

class TestCacheUtilRunner(CreateDefaultAssetCacheMixin, unittest.TestCase):

    def setUp(self):
        from baiji.pod.runners.cache_util_runner import CacheUtilRunner
        super(TestCacheUtilRunner, self).setUp()
        self.runner = CacheUtilRunner(self.cache)

    @mock.patch('__builtin__.print')
    def test_loc(self, mock_print):
        self.runner.main(['loc'])
        mock_print.assert_called_with(self.cache.config.cache_dir)
