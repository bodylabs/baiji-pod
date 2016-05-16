import os
from baiji import s3


class CachedPath(str):
    pass


class CacheFile(object):
    def __init__(self, static_cache, path, bucket=None):
        self.config = static_cache.config

        if s3.path.isremote(path):
            parsed_path = s3.path.parse(path)
            self.path = parsed_path.path
            self.bucket = parsed_path.netloc
        else:
            if static_cache.is_cachefile(path):
                self.path = static_cache.un_sc(path)
                self.bucket = path.replace(self.config.cache_dir, '').split(os.sep)[0]
            else:
                self.path = path
                self.bucket = bucket if bucket != None else self.config.default_bucket

        if not self.path.startswith('/'):
            self.path = '/' + self.path

    @property
    def local(self):
        local_path = self.path[1:]
        if os.sep != '/':
            local_path = local_path.replace('/', os.sep)
        full_local_path = os.path.join(self.config.cache_dir, self.bucket, local_path)
        return CachedPath(full_local_path)

    @property
    def remote(self):
        return 's3://{}{}'.format(self.bucket, self.path)

    @property
    def timestamp_file(self):
        return os.path.join(
            self.config.cache_dir,
            '.timestamps',
            self.bucket,
            self.path[1:])

    @property
    def timestamp(self):
        return os.path.getmtime(self.timestamp_file)

    @property
    def age(self):
        import time
        if not os.path.exists(self.timestamp_file): # unknown
            return float('inf')
        return time.mktime(time.localtime()) - self.timestamp

    @property
    def size(self):
        stat = os.stat(self.local)
        return stat.st_size

    def update_timestamp(self):
        from baiji.util.shutillib import mkdir_p
        if not os.path.exists(self.timestamp_file):
            mkdir_p(os.path.dirname(self.timestamp_file))
        open(self.timestamp_file, 'w').close()

    def invalidate(self):
        from baiji.pod.util.shutillib import remove_tree
        remove_tree(self.timestamp_file)

    @property
    def is_outdated(self):
        if self.bucket in self.config.immutable_buckets:
            return False
        timeout = self.config.timeout
        if not timeout: # Never check
            return False
        if timeout == 0: # always check
            return True
        return self.age > timeout

    def download(self, verbose=True):
        if not s3.exists(self.remote):
            raise s3.KeyNotFound('{} not found on s3'.format(self.remote))
        s3.cp(self.remote, self.local, force=True, progress=verbose, validate=True)
        self.update_timestamp()

    @property
    def is_cached(self):
        return os.path.exists(self.local)

    def remove_cached(self):
        from baiji.pod.util.shutillib import remove_tree
        self.invalidate()
        remove_tree(self.local)


class AssetCache(object):
    KeyNotFound = s3.KeyNotFound

    def __init__(self, config):
        from env_flag import env_flag
        self.config = config
        self.verbose = not env_flag('PRODUCTION')

    @classmethod
    def create_default(cls):
        from baiji.pod.config import Config
        config = Config()
        return cls(config)

    def _raise_cannot_get_needed_file(self, cache_file, reason):
        from baiji.config import credentials
        from baiji.exceptions import AWSCredentialsMissing
        from baiji.pod.util import yaml
        from bodylabs.util.internet import InternetUnreachableError
        from bodylabs.util.paths import core_path

        msg = 'Tried to access {} from cache '.format(cache_file.remote)
        msg += 'but it was not in the cache '
        msg += '(expected to see it at {}). '.format(cache_file.local)
        msg += 'Tried to download it, '
        if reason is InternetUnreachableError:
            msg += "but we can't contact s3."
        elif reason is AWSCredentialsMissing:
            msg += 'but there are no s3 access credentials.'
        else:
            msg += 'but something went wrong.'

        try:
            _ = credentials.key
        except AWSCredentialsMissing:
            msg += " We've written this to a list of files you're missing in missing_assets.txt"
            missing_asset_log = os.path.join(core_path(), 'missing_assets.yaml')
            missing_assets = []
            try:
                missing_assets = yaml.load(missing_asset_log)
            except s3.KeyNotFound:
                pass
            missing_assets.append(cache_file.remote)
            missing_assets = sorted(list(set(missing_assets)))
            yaml.dump(missing_assets, missing_asset_log)

        raise reason(msg)

    def __call__(self, path, bucket=None, force_check=False, verbose=None, stacklevel=1):
        '''
        Algorithm:

        - If we have no local copy: download, mark it as checked now, and
          return its path.
        - If it's less than `config.TIMEOUT` since it was last checked,
          return its path.
        - If the md5 of the local file matches the md5 of the remote file, mark
          it as checked now and return it's path.
        - Otherwise it's out of date and changed on s3: download, mark it as
          checked now, and return it's path.

        stacklevel: When `verbose` is `True`, how far up the stack to look when
            printing debug output. 1 means the immediate caller, 2 its caller,
            and so on. Useful when calls to sc() are wrapped, such as in vc().
        '''
        import socket
        from baiji.exceptions import AWSCredentialsMissing
        from bodylabs.util.internet import assert_internet_reachable, InternetUnreachableError

        if verbose is None: # in most cases, we'll simply use the default for this cache object
            verbose = self.verbose
        def maybe_print(message):
            from bodylabs.util.inspectlib import stack_frame_info
            if verbose:
                # stacklevel+2: one for `maybe_print`, one for `__call__`
                where = stack_frame_info(stacklevel + 2).pretty
                print message + ' - ' + where

        cache_file = CacheFile(static_cache=self, path=path, bucket=bucket)

        if not cache_file.is_cached:
            try:
                assert_internet_reachable()
                maybe_print('Downloading missing file {}'.format(cache_file.remote))
                cache_file.download(verbose=verbose)
            except (socket.gaierror, InternetUnreachableError):
                self._raise_cannot_get_needed_file(cache_file, InternetUnreachableError)
            except AWSCredentialsMissing:
                self._raise_cannot_get_needed_file(cache_file, AWSCredentialsMissing)
        elif force_check or cache_file.is_outdated:
            try:
                assert_internet_reachable()
                if s3.etag(cache_file.remote) == s3.etag(cache_file.local):
                    cache_file.update_timestamp()
                else:
                    maybe_print('Downloading outdated file {}'.format(cache_file.remote))
                    cache_file.download(verbose=verbose)
            except (socket.gaierror, InternetUnreachableError, AWSCredentialsMissing):
                maybe_print(
                    ("File {} may be outdated, but we can't contact s3, " +
                     "so let's assume it's ok").format(cache_file.remote))
        return cache_file.local

    def invalidate(self, path, bucket=None):
        CacheFile(static_cache=self, path=path, bucket=bucket).invalidate()

    def invalidate_all(self):
        import shutil
        shutil.rmtree(
            os.path.join(self.config.cache_dir, '.timestamps'),
            ignore_errors=True)

    def delete(self, path, bucket=None):
        CacheFile(static_cache=self, path=path, bucket=bucket).remove_cached()

    def is_cachefile(self, path):
        return isinstance(path, CachedPath) or \
            os.path.expanduser(path).startswith(self.config.cache_dir)

    def un_sc(self, path):
        '''un_sc(sc(foo)) == foo'''
        import re
        if self.is_cachefile(path):
            # Nested calls to sc
            path = path.replace(self.config.cache_dir, '')
            # Remove leading bucket
            path = re.match(r'[^/\\]*(/|\\)(.*)', path).groups()[1]
        return path

    def ls(self):
        files_in_cache = []
        for bucket in os.listdir(self.config.cache_dir):
            bucket_path = os.path.join(self.config.cache_dir, bucket)
            if os.path.isdir(bucket_path) and bucket != '.timestamps':
                for root, _, files in os.walk(bucket_path):
                    for name in files:
                        if name not in ['.DS_Store']:
                            cache_file = CacheFile(
                                static_cache=self,
                                path=os.path.join(root, name),
                                bucket=bucket)
                            files_in_cache.append(cache_file)
        return files_in_cache