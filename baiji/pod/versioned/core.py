import os
from baiji import s3
from bodylabs.util.decorators import cached_property


class VersionedCache(object):
    '''
    The idea here is to have a cache system like sc where you specify a file by path
    in a particular bucket. Unlike sc here we might have many different versions of a
    given file and when you ask for it you get the version that's specified in your
    manifest.json file. This way we can have multiple versions of an asset living on s3
    and each version of the code can have an identifier checked in that tells it which
    version it's compatible with.

    manifest.json looks like this:
        {
            "/foo/bar.csv": "1.2.5",
            "/foo/bar.json": "0.1.6"
        }
    To get the file (or rather a local path to the file, like sc), you call
        vc("/foo/bar.csv")
    There might be several versions of this file in the bucket, but you have a particular
    version number committed, so you know you'll get the version you're expecting.

    When you're developing though, you often want to try out variations on a file before
    committing to a particular one. Rather than incrementing the patch level over and over,
    you can set manifest.json to include an absolute path:
        "/foo/bar.csv": "/Users/me/Desktop/foo.obj",
    This can be either a local or an s3 path; use local if you're iterating by yourself,
    and s3 (in bodylabs-assets or something) if you want to try something on staging, for
    example.

    The bucket bodylabs-versioned-assets is intended as immutable; nothing there should ever
    be changed or deleted. Only new versions added.

    Adding a new file to versioning or updating a versioned file is best done using the vc
    command line tool:
        vc add /foo/bar.csv ~/Desktop/bar.csv
        vc update --major /foo/bar.csv ~/Desktop/new_bar.csv
        vc update --minor /foo/bar.csv ~/Desktop/new_bar.csv
        vc update --patch /foo/bar.csv ~/Desktop/new_bar.csv

    Evenutually we should use semantic_version for parsing and comparing version strings...

    A VersionedCache object is specific to a manifest file and a bucket; the default vc object
    is tied to the core/bodylabs/cache/manifest.json file and the bodylabs-versioned-assets
    bucket.
    '''
    KeyNotFound = s3.KeyNotFound

    def __init__(self, static_cache, manifest_path, bucket):
        '''
        manifest_path: A path to the vesioned cache manifest JSON file.
        bucket: The bucket containing the versioned assets.
        '''
        self.sc = static_cache
        # If we did any dynamic creation of the version manifest, we'd do it here
        self.manifest_path = manifest_path
        self.bucket = bucket

    def __call__(self, *args, **kwargs):
        return self.cached_file(*args, **kwargs)

    def cached_file(self, path, version=None, verbose=None):
        '''
        Default version is manifest version. In almost all cases you want to pass nothing for version.
        If verbose is left at None, sc uses its global default.
        '''
        if not self.is_versioned(path):
            raise self.KeyNotFound("%s is not a versioned path" % path)
        uri = self.uri(path, version)
        if not s3.exists(uri):
            raise self.KeyNotFound('{} is not cached for version {}'.format(
                path, version))
        return self.sc(uri, verbose=verbose, stacklevel=2)

    @cached_property
    def manifest(self):
        from bodylabs.serialization import json
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
        from bodylabs.serialization import json
        path = self.normalize_path(path)
        manifest = json.load(self.manifest_path)
        manifest[path] = version
        json.dump(manifest, self.manifest_path, sort_keys=True, indent=4)
        if hasattr(self, '_cache') and 'manifest' in self._cache:
            del self._cache['manifest']

    def uri(self, path, version=None, allow_local=True, suffixes=None, bucket=None):
        '''
        Default version is manifest version
        '''
        path = self.normalize_path(path)
        if version is None:
            version = self.manifest_version(path)
        if bucket is None:
            bucket = self.bucket
        if self.version_number_is_valid(version):
            base_path, ext = os.path.splitext(path)
            suffixes = '.' + '.'.join(suffixes) if suffixes is not None and len(suffixes) > 0 else ''
            return "s3://" + bucket + base_path + "." + version + suffixes + ext
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
        raise ValueError("No version string found in %s" % path)

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
            raise ValueError("%s is already versioned; did you mean vc.update?", path)
        if version is None:
            version = "1.0.0"
        else:
            version = self.normalize_version_number(version)
            if not self.version_number_is_valid(version):
                raise ValueError("invalid version %s, always use versions of the form N.N.N" % version)
        s3.cp(local_file, self.uri(path, version), progress=verbose)
        self.update_manifest(path, version)

    def ls_remote(self):
        '''
        Return a list of keys on the remote server.

        TODO This could return the versions and create date too.

        '''
        paths = s3.ls("s3://" + self.bucket)
        parsed = [self.parse(path) for path in paths]
        # parsed is a list of key, version tuples
        return sorted(set([key for key, _ in parsed]))

    def versions_avaliable(self, path):
        import semantic_version
        path = self.normalize_path(path)
        base_path, ext = os.path.splitext(path)
        versions = filter(lambda path: os.path.splitext(path)[1] == ext, s3.ls("s3://" + self.bucket + base_path))
        versions = sorted([semantic_version.Version(self.extract_version(v)) for v in versions])
        versions = [str(v) for v in versions]
        return versions

    def latest_available_version(self, path):
        versions = self.versions_avaliable(path)
        if len(versions) > 0:
            return str(versions[-1])
        else:
            return None

    def normalize_version_number(self, version):
        import semantic_version
        if isinstance(version, basestring):
            version = semantic_version.Version(version, partial=True)
        if version.major == None:
            version.major = 0
        if version.minor == None:
            version.minor = 0
        if version.patch == None:
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
        if self.version_matches_spec(version, ">="+min_version):
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
            raise self.KeyNotFound("%s is not a versioned path; did you mean vc.add?" % path)
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
                raise ValueError("Umm.... what did you want to update the version to?")
            if min_version is not None:
                version = self.apply_min_version(version, min_version)
            version = str(version)
        else:
            version = self.normalize_version_number(version)
        if not self.version_number_is_valid(version):
            raise ValueError("invalid version %s, always use versions of the form N.N.N" % version)
        latest_version = self.latest_available_version(path)
        if not semantic_version.Version(version) > semantic_version.Version(latest_version):
            raise ValueError("version numbers must be strictly increasing. You specified %s but there is already a %s", version, latest_version)
        s3.cp(local_file, self.uri(path, version), progress=verbose)
        self.update_manifest(path, version)

    def update_major(self, path, local_file, verbose=False):
        self.update(path, local_file, major=True, verbose=verbose)

    def update_minor(self, path, local_file, verbose=False):
        self.update(path, local_file, minor=True, verbose=verbose)

    def update_patch(self, path, local_file, verbose=False):
        self.update(path, local_file, patch=True, verbose=verbose)

    def add_or_update(self, path, local_file, version=None, major=False, minor=False, patch=False, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(path, local_file, version=version, major=major, minor=minor, patch=patch, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=version, verbose=verbose)

    def add_or_update_major(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(path, local_file, major=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)

    def add_or_update_minor(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(path, local_file, minor=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)

    def add_or_update_patch(self, path, local_file, min_version=None, verbose=False):
        if self.is_versioned(path):
            self.update(path, local_file, patch=True, min_version=min_version, verbose=verbose)
        else:
            self.add(path, local_file, version=min_version, verbose=verbose)
