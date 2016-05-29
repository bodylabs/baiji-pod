import os
from baiji import s3
from cached_property import cached_property


class VersionedCache(object):
    '''
    Encapsulate a repository of version-tracked files, which are backed by a
    single S3 bucket and indexed by a single manifest file.

    Delegates the caching and file management to the underlying asset cache.
    '''
    KeyNotFound = s3.KeyNotFound

    def __init__(self, cache, manifest_path, bucket):
        '''
        cache: An instance of AssetCache.
        manifest_path: A path to the vesioned cache manifest JSON file.
        bucket: The bucket containing the versioned assets.
        '''
        self.cache = cache
        self.manifest_path = manifest_path
        self.bucket = bucket

    def __call__(self, *args, **kwargs):
        return self.cached_file(*args, **kwargs)

    def cached_file(self, path, version=None, verbose=None):
        '''
        Default version is manifest version. In almost all cases you want to
        pass nothing for version.

        If verbose is left at None, uses the underlying asset cache's global
        default.
        '''
        if not self.is_versioned(path):
            raise self.KeyNotFound('{} is not a versioned path'.format(path))
        uri = self.uri(path, version)
        try:
            # TODO Put a test around this magic number.
            return self.cache(uri, verbose=verbose, stacklevel=3)
        except s3.KeyNotFound:
            raise self.KeyNotFound('{} is not cached for version {}'.format(
                path, version))

    @cached_property
    def manifest(self):
        from baiji.pod.util import json
        return json.load(self.manifest_path)

    @property
    def manifest_files(self):
        return self.manifest.keys()

    def manifest_version(self, path):
        path = self.normalize_path(path)
        return self.manifest[path]

    def is_versioned(self, path):
        path = self.normalize_path(path)
        return path in self.manifest

    def update_manifest(self, path, version):
        from baiji.pod.util import json

        path = self.normalize_path(path)

        manifest = json.load(self.manifest_path)
        manifest[path] = version
        json.dump(manifest, self.manifest_path, sort_keys=True, indent=4)

        try:
            del self.__dict__['manifest']
        except KeyError:
            pass

    def uri(self, path, version=None, allow_local=True, suffixes=None):
        '''
        Default version is manifest version
        '''
        path = self.normalize_path(path)

        if version is None:
            version = self.manifest_version(path)

        if self.version_number_is_valid(version):
            base_path, ext = os.path.splitext(path)
            suffixes = '.' + '.'.join(suffixes) if suffixes is not None and len(suffixes) > 0 else ''
            return 's3://' + self.bucket + base_path + '.' + version + suffixes + ext
        elif allow_local and s3.exists(version):
            # version here is a local or s3 path
            return version
        else:
            raise self.KeyNotFound("File not found: %s", version)

    def normalize_path(self, path):
        if not path.startswith('/'):
            path = '/' + path
        return path

    def version_number_is_valid(self, version):
        import re
        return re.match(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version)

    def extract_version(self, path):
        import re
        x = re.match(r'.*?\.((0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*))(\.[^\.]*)?', path)
        if x is not None:
            return x.groups()[0]
        raise ValueError('No version string found in {}'.format(path))

    def parse(self, path):
        '''
        Parse a path and return a tuple: key, version

        '''
        version = self.extract_version(path)
        position = path.rfind(version)
        key = path[:position].rstrip('.') + path[position + len(version):]
        return key, version

    def add(self, path, local_file, version=None, verbose=False):
        path = self.normalize_path(path)
        if self.is_versioned(path):
            raise ValueError('{} is already versioned; did you mean vc.update?'.format(path))

        if version is None:
            version = '1.0.0'
        else:
            version = self.normalize_version_number(version)
            if not self.version_number_is_valid(version):
                raise ValueError('invalid version {}, always use versions of the form N.N.N'.format(version))

        s3.cp(local_file, self.uri(path, version), progress=verbose)
        self.update_manifest(path, version)

    def ls_remote(self):
        '''
        Return a list of keys on the remote server.

        TODO This could return the versions and create date too.

        '''
        paths = s3.ls('s3://' + self.bucket)
        parsed = [self.parse(path) for path in paths]
        # parsed is a list of key, version tuples.
        return sorted(set([key for key, _ in parsed]))

    def versions_available(self, path):
        import semantic_version

        path = self.normalize_path(path)
        base_path, ext = os.path.splitext(path)

        versions = filter(lambda path: os.path.splitext(path)[1] == ext, s3.ls('s3://' + self.bucket + base_path))
        versions = sorted([semantic_version.Version(self.extract_version(v)) for v in versions])
        versions = [str(v) for v in versions]
        return versions

    def latest_available_version(self, path):
        versions = self.versions_available(path)
        if len(versions) > 0:
            return str(versions[-1])
        else:
            return None

    def normalize_version_number(self, version):
        import semantic_version

        if isinstance(version, basestring):
            version = semantic_version.Version(version, partial=True)

        if version.major is None:
            version.major = 0
        if version.minor is None:
            version.minor = 0
        if version.patch is None:
            version.patch = 0
        version.prerelease = []
        version.build = []

        return str(version)

    def manifest_matches_spec(self, path, spec):
        if not self.is_versioned(path):
            return False
        return self.version_matches_spec(self.manifest_version(path), spec)

    def latest_matches_spec(self, path, spec):
        version = self.latest_available_version(path)
        if version is None:
            return False
        return self.version_matches_spec(version, spec)

    def version_matches_spec(self, version, spec):
        import semantic_version
        return semantic_version.Spec(str(spec)).match(semantic_version.Version(str(version)))

    def apply_min_version(self, version, min_version):
        if self.version_matches_spec(version, '>='+min_version):
            return version
        else:
            return self.normalize_version_number(min_version)

    def next_version_number(self, path, min_version=None):
        '''
        Gives the next possible version number.
        Will bump patch unless min_version is given, in which case the new version will be at least min_version.
        min_version may be partial.
        eg: if vc.latest_available_version('/foo') == 3.4.5:
            vc.next_version_number('/foo') => 3.4.6
            vc.next_version_number('/foo', '3') => 3.4.6
            vc.next_version_number('/foo', '4') => 4.0.0
            vc.next_version_number('/foo', '3.4') => 3.4.6
            vc.next_version_number('/foo', '3.7') => 3.7.0
            vc.next_version_number('/foo', '3.4.13') => 3.4.13
        '''
        import semantic_version
        version = semantic_version.Version(self.latest_available_version(path))
        version.patch += 1
        if min_version is not None:
            version = self.apply_min_version(version, min_version)
        return str(version)

    def update(self, path, local_file, version=None, major=False, minor=False, patch=False, min_version=None, verbose=False):
        """Update path in vc by local_file.

        There are two ways to specify a new version:
            1. set version = 'x.x.x' directly
            2. set major, minor or patch to be True.
        Version has a higher priority than major, minor or patch.

        When using major, minor or patch to bump version, min_version can be used. The final version of this file
        will be updated to be max(min_version, version_from_major_minor_or_patch).
        """
        import semantic_version

        path = self.normalize_path(path)

        latest_version = self.latest_available_version(path)
        if latest_version is None:
            raise self.KeyNotFound('{} is not a versioned path; did you mean vc.add?'.format(path))

        if version is None:
            version = semantic_version.Version(latest_version)
            if major:
                version.major += 1
                version.minor = 0
                version.patch = 0
            elif minor:
                version.minor += 1
                version.patch = 0
            elif patch:
                version.patch += 1
            else:
                raise ValueError('Umm.... what did you want to update the version to?')
            if min_version is not None:
                version = self.apply_min_version(version, min_version)
            version = str(version)
        else:
            version = self.normalize_version_number(version)

        if not self.version_number_is_valid(version):
            raise ValueError('Invalid version {}, always use versions of the form N.N.N'.format(version))

        latest_version = self.latest_available_version(path)
        if semantic_version.Version(version) <= semantic_version.Version(latest_version):
            raise ValueError('Version numbers must be strictly increasing. You specified {} but there is already a {}'.format(version, latest_version))

        s3.cp(local_file, self.uri(path, version), progress=verbose)
        self.update_manifest(path, version)

    def update_major(self, path, local_file, verbose=False):
        self.update(path, local_file, major=True, verbose=verbose)

    def update_minor(self, path, local_file, verbose=False):
        self.update(path, local_file, minor=True, verbose=verbose)

    def update_patch(self, path, local_file, verbose=False):
        self.update(path, local_file, patch=True, verbose=verbose)

    def add_or_update(self, path, local_file,
                      version=None, major=False, minor=False, patch=False,
                      min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(
                path, local_file,
                version=version, major=major, minor=minor, patch=patch,
                min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=version, verbose=verbose)

    def add_or_update_major(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(
                path, local_file,
                major=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)

    def add_or_update_minor(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(
                path, local_file,
                minor=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)

    def add_or_update_patch(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(
                path, local_file,
                patch=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)

    def sync(self, destination):
        for f in self.manifest_files:
            target = s3.path.join(destination, f[1:])
            print 'Copying {} version {} to {}'.format(
                f,
                self.manifest_version(f),
                target)
            s3.cp(self(f), target, force=True)
