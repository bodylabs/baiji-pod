'''
Env Variables:

 - STATIC_CACHE_DIR: Where we store the cache. Defaults to `~/.baiji_cache`.
 - STATIC_CACHE_DEFAULT_BUCKET: If no bucket is given, we default to bodylabs-assets.
 - STATIC_CACHE_TIMEOUT: Time we assume the file is good for. If an integer,
     it's in seconds; if not an integer, means we never check for changes.
     Defaults to one day.

'''

import os

class Config(object):
    '''
    To declare your own defaults, you may subclass this config and inject it
    into the StaticCache object.
    '''

    DEFAULT_BUCKET = None
    CACHE_DIR = os.path.expanduser('~/.baiji_static_cache')
    TIMEOUT = 86400  # One day.
    GARBAGE_COLLECTION_TIMEOUT = 2592000  # 30 days.
    IMMUTABLE_BUCKETS = []

    @property
    def default_bucket(self):
        return os.getenv('STATIC_CACHE_DEFAULT_BUCKET', self.DEFAULT_BUCKET)

    @property
    def cache_dir(self):
        cache_dir = os.getenv('STATIC_CACHE_DIR', self.CACHE_DIR)
        if cache_dir[-1] != os.sep:
            cache_dir += os.sep
        return cache_dir

    @property
    def timeout(self):
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
