class VersionedCacheUploader(object):
    '''
    `VersionedCacheUploader` is intended to be used as a context manager:

        with VersionedCacheUploader(vcpath) as f:
            # write content to the file

    Upon exiting the `with` block, the temporary file is uploaded to `vcpath`
    then deleted.

    Note that there is a parallel tool at
    bodylabs.serialization.temporary.Tempfile that is designed for the case
    where you want to do more with the file than merely upload it to vc.
    '''

    def __init__(self, versioned_cache, vcpath, version=None, major=False, minor=False, patch=False,
                 min_version=None, verbose=False):
        self.vc = versioned_cache
        self.vcpath = vcpath
        self.version = version
        self.major = major
        self.minor = minor
        self.patch = patch
        self.min_version = min_version
        self.verbose = verbose
        self.tf = None

    def __enter__(self):
        import tempfile
        self.tf = tempfile.NamedTemporaryFile(delete=True)
        return self.tf

    def __exit__(self, exception_type, exception_value, traceback):
        self.vc.add_or_update(
            self.vcpath,
            self.tf.name,
            version=self.version,
            major=self.major,
            minor=self.minor,
            patch=self.patch,
            min_version=self.min_version,
            verbose=self.verbose)

        # When we close the file here, NamedTemporaryFile deletes it as well.
        self.tf.close()
