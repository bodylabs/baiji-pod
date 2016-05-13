baiji-pod
=========

Static caching of files from Amazon S3, using [baiji][].

[baiji]: http://github.com/bodylabs/baiji


Features
--------

- Static cache supports loading any S3 path
- Versioned cache supports loading S3 paths from a specific bucket, using
  version numbers pinned in a manifest
- Both caches support pre-filling from a manifest file
- Supports Python 2.7
- Supports OS X, Linux, and Windows
- Tested and production-hardened


Examples
--------

### The static cache

Use as:

    from bodylabs.cache import sc
    sc('path') # defauly remote bucket of bodylabs-assets
    sc('path', bucket='bodylabs-foo')
    sc('s3://explicit-bucket-name/path')

In all cases, it returns a string which is a valid local path.

Algorithm:

- If we have no local copy: download, mark it as checked now, and return it's path.
- If it's less than `STATIC_CACHE_TIMEOUT` since it was last checked, return
  its path.
- If the md5 of the local file matches the md5 of the remote file, mark it as
  checked now and return it's path.
- Otherwise it's out of date and changed on s3: download, mark it as checked
  now, and return it's path.

It is safe to call multiple times &ndash; `sc(sc('path'))` will behave correctly.


Development
-----------

```sh
pip install -r requirements_dev.txt
rake unittest
rake lint
```


Contribute
----------

- Issue Tracker: https://github.com/bodylabs/baiji-pod/issues
- Source Code: https://github.com/bodylabs/baiji-pod

Pull requests welcome!


Support
-------

If you are having issues, please let us know.


License
-------

The project is licensed under the Apache license, version 2.0.
