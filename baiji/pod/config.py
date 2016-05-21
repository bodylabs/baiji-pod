import os

class Config(object):
    '''
    To configure baiji-pod, you can override this config (by subclassing
    or instantiating a singleton, and setting your own versions of these
    variables. Pass your config object to the AssetCache constructor.

    You can also make your own copy of the `baiji-cache` and `vc` runners
    which use your configuration.

    Alternatively, you can configure baiji-pod using environment variables.
    '''
    CACHE_DIR = os.path.expanduser('~/.baiji_cache')
    TIMEOUT = 86400  # == one day.
    IMMUTABLE_BUCKETS = []
    DEFAULT_BUCKET = None
    VERBOSE = True
    NUM_PREFILL_PROCESSES = 12

    @property
    def cache_dir(self):
        '''
        Where we store the cache. Defaults to `~/.baiji_cache`.
        '''
        cache_dir = os.getenv('STATIC_CACHE_DIR', self.CACHE_DIR)
        if cache_dir[-1] != os.sep:
            cache_dir += os.sep
        return cache_dir

    @property
    def timeout(self):
        '''
        Time we assume the file is good for. If an integer, it's in seconds;
        if not an integer, means we never check for changes. Defaults to one
        day.
        '''
        try:
            return int(os.getenv('STATIC_CACHE_TIMEOUT', self.TIMEOUT))
        except ValueError:
            return None

    @property
    def immutable_buckets(self):
        '''
        If you set this, it's a : seperated list, like a path.
        These buckets are cached once only and never checked for updates.
        Note though that you can still use `sc(..., force_check=True)` to make an update
        happen if there's ever cause.
        '''
        try:
            return os.environ['STATIC_CACHE_IMMUTABLE_BUCKETS'].split(':')
        except KeyError:
            return self.IMMUTABLE_BUCKETS

    @property
    def default_bucket(self):
        '''
        The default bucket, used to handle an call to sc on a path with no
        bucket name. Deprecated.
        '''
        return os.getenv('STATIC_CACHE_DEFAULT_BUCKET', self.DEFAULT_BUCKET)

    @property
    def verbose(self):
        '''
        Whether the asset cache should print activity logs.
        '''
        return self.VERBOSE

    @property
    def num_prefill_processes(self):
        '''
        The number of parallel processes to use during prefill.
        '''
        return self.NUM_PREFILL_PROCESSES
