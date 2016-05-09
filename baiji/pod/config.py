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
    def gc_timeout(self):
        try:
            return int(os.getenv('STATIC_CACHE_GARBAGE_COLLECTION_TIMEOUT', self.GARBAGE_COLLECTION_TIMEOUT))
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


class SCExemptList(object):
    def __init__(self, exempt_path=None):
        '''
        By default, SCExemptList uses the list of assets checked in to core at
        bodylabs/cache/sc_gc.conf.yaml. To use an alternate list of files,
        specify the path with `exempt_path`.
        '''
        from baiji.pod.util import yaml

        if exempt_path is not None:
            self._global_exempt_path = exempt_path
        else:
            self._global_exempt_path = os.path.join(os.path.dirname(__file__), 'sc_gc.conf.yaml')

        self._local_exempt_path = os.path.join(os.path.dirname(__file__), 'sc_gc.local.yaml')
        self._global_exempt = yaml.load(self._global_exempt_path)
        self._local_exempt = None
        self._load()

    def _load(self):
        from baiji.pod.util import yaml

        self._local_exempt = []

        if os.path.exists(self._local_exempt_path):
            local_exempt = yaml.load(self._local_exempt_path)
            if local_exempt:
                self._local_exempt = local_exempt

    def _save(self):
        from baiji.pod.util import yaml
        yaml.dump(self._local_exempt, self._local_exempt_path)

    def add(self, remote):
        self._load()
        self._local_exempt.append(remote)
        self._save()

    def remove(self, remote):
        self._load()
        try:
            self._local_exempt.remove(remote)
        except ValueError:
            # remove something that wasn't there; don't really care
            pass
        self._save()

    def _all(self):
        return self._global_exempt + self._local_exempt

    def __len__(self):
        return len(self._all())

    def __getitem__(self, key):
        return self._all()[key]

    def __iter__(self):
        return iter(self._all())

    def __contains__(self, item):
        return item in self._all()
