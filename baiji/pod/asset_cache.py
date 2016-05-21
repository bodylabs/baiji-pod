import os
from baiji import s3


class CachedPath(unicode):
    pass


class CacheFile(object):
    def __init__(self, static_cache, path, bucket=None):
        self.config = static_cache.config

        if s3.path.isremote(path):
            parsed_path = s3.path.parse(path)
            self.path = parsed_path.path
            self.bucket = parsed_path.netloc
            if bucket is not None:
                raise ValueError('When providing an s3 path, do not use the bucket argument')
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
        try:
            return os.path.getmtime(self.timestamp_file)
        except OSError as e:
            import errno
            if e.errno == errno.ENOENT:
                return None
            else:
                raise

    @property
    def age(self):
        import time
        try:
            return time.mktime(time.localtime()) - self.timestamp
        except TypeError: # float - NoneType
            return float('inf')

    @property
    def size(self):
        try:
            return os.stat(self.local).st_size
        except OSError as e:
            import errno
            if e.errno == errno.ENOENT:
                return None
            else:
                raise

    def update_timestamp(self):
        from baiji.util.shutillib import mkdir_p
        if not os.path.exists(self.timestamp_file):
            mkdir_p(os.path.dirname(self.timestamp_file))
        open(self.timestamp_file, 'w').close()

    def invalidate(self):
        from baiji.pod.util.shutillib import remove_file
        remove_file(self.timestamp_file)

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
        try:
            s3.cp(self.remote, self.local, force=True, progress=verbose, validate=True)
        except s3.KeyNotFound:
            raise s3.KeyNotFound('{} not found on s3'.format(self.remote))
        self.update_timestamp()

    @property
    def is_cached(self):
        return os.path.exists(self.local)

    def remove_cached(self):
        from baiji.pod.util.shutillib import remove_file
        self.invalidate()
        remove_file(self.local)


class AssetCache(object):
    KeyNotFound = s3.KeyNotFound

    def __init__(self, config):
        self.config = config

    @classmethod
    def create_default(cls):
        from baiji.pod.config import Config
        config = Config()
        return cls(config)

    def _raise_cannot_get_needed_file(self, cache_file, reason):
        from baiji.config import credentials
        from baiji.exceptions import AWSCredentialsMissing
        from baiji.pod.util import yaml
        from baiji.pod.util.reachability import InternetUnreachableError

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
            missing_asset_log_path = os.path.join(
                self.config.cache_dir,
                'missing_assets.yaml')
            msg += " We've written this to a list of files you're missing in {}".format(
                missing_asset_log_path)
            try:
                missing_assets = yaml.load(missing_asset_log_path)
            except IOError as e:
                import errno
                if e.errno == errno.ENOENT:
                    missing_assets = []
                else:
                    raise
            missing_assets.append(cache_file.remote)
            missing_assets = sorted(list(set(missing_assets)))
            # This yaml.dump doesn't create intermediate directories, but
            # because s3.cp creates interposing directories, we know the cache
            # dir already exists at this point.
            yaml.dump(missing_assets, missing_asset_log_path)

        raise reason(msg)

    def __call__(self, path, bucket=None, force_check=False, verbose=None, stacklevel=1):
        '''
        Algorithm:

        - If we have no local copy: download, mark it as checked now, and
          return its path.
        - If it's less than `config.TIMEOUT` since it was last checked,
          return its path.
        - If the md5 of the local file matches the md5 of the remote file, mark
          it as checked now and return it's path. For remote paths, the etag
          contains the md5 of the contents, except for multipart uploads. In
          baiji, files over 5gb are multipart uploaded, and use an algorithm
          shared between baiji and s3 to get an etag hash based on md5.
        - Otherwise it's out of date and changed on s3: download, mark it as
          checked now, and return it's path.

        stacklevel: When `verbose` is `True`, how far up the stack to look when
            printing debug output. 1 means the immediate caller, 2 its caller,
            and so on. Useful when calls to cache() are wrapped, such as in vc().
        '''
        import socket
        from baiji.exceptions import AWSCredentialsMissing
        from baiji.pod.util.reachability import assert_internet_reachable, InternetUnreachableError

        if verbose is None: # in most cases, we'll simply use the default for this cache object
            verbose = self.config.verbose
        def maybe_print(message):
            from harrison.util.inspectlib import stack_frame_info
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
        cf = CacheFile(static_cache=self, path=path, bucket=bucket)
        if os.path.isdir(cf.local): # we're dealing with a tree, not an actual CacheFile
            from baiji.pod.util.shutillib import remove_tree
            remove_tree(cf.timestamp_file)
        else:
            cf.invalidate()

    def invalidate_all(self):
        from baiji.pod.util.shutillib import remove_tree
        remove_tree(os.path.join(self.config.cache_dir, '.timestamps'))

    def delete(self, path, bucket=None):
        CacheFile(static_cache=self, path=path, bucket=bucket).remove_cached()

    def is_cachefile(self, path):
        return isinstance(path, CachedPath) or \
            os.path.expanduser(path).startswith(self.config.cache_dir)

    def un_sc(self, path):
        '''un_sc(sc(foo)) == foo'''
        if self.is_cachefile(path):
            # Nested calls to sc
            path = path.replace(self.config.cache_dir, '')
            # Remove leading bucket
            path = path.split(os.sep, 1)[1]
        return path

    def ls(self):
        for bucket in os.listdir(self.config.cache_dir):
            bucket_path = os.path.join(self.config.cache_dir, bucket)
            if os.path.isdir(bucket_path) and bucket != '.timestamps':
                for root, _, files in os.walk(bucket_path):
                    for name in files:
                        if name not in ['.DS_Store']:
                            yield CacheFile(
                                static_cache=self,
                                path=os.path.join(root, name),
                                bucket=bucket)
