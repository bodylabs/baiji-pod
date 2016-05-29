baiji-pod
=========

Versioned-tracked assets and a low-level asset cache for Amazon S3, using
[baiji][].

[baiji]: http://github.com/bodylabs/baiji


Features
--------

- Versioned cache for version-tracked assets
    - Creates a new file each time it changes
    - Using a checked-in manifest, each revision of the code is pinned to a
      given version of the file
    - Convenient CLI for pushing updates
- Low-level asset cache, for any S3 path
    - Assets are stored locally, and revalidated after a timeout
- Prefill tool populates the caches with a list of needed assets
- Supports Python 2.7
- Supports OS X, Linux, and Windows
    - A few dev features only work on OS X
- Tested and production-hardened


### The versioned cache

The versioned cache provides access to a repository of files. The changes to
those files are tracked and identified with to a semver-like version number.

To use the versioned cache, you need a copy of a manifest file, which lists
all the versioned paths and the latest version of each one. When you request a
file from the cache, it consults this manifest file to determine the correct
version. The versioned cache delegates loading to the underlying asset cache.

The versioned cache was designed for compute assets: chunks of data which are
used in code. When the manifest is checked in with the code, it pins the
version of each asset. If the asset is subsequently updated, that revision
of the code will continue to get the version it's expecting.

The bucket containing the versioned assets is intended to be immutable.
Nothing there should ever be changed or deleted. Only new versions added.

The manifest looks like this:

```json
{
    "/foo/bar.csv": "1.2.5",
    "/foo/bar.json": "0.1.6"
}
```

To load a versioned asset:

```
import json
from baiji.pod import AssetCache
from baiji.pod import Config
from baiji.pod import VersionedCache

config = Config()
# Improve performance by assuming the bucket is immutable.
config.IMMUTABLE_BUCKETS = ['my-versioned-assets']

vc = VersionedCache(
    asset_cache=AssetCache(config),
    manifest_path='versioned_assets.json',
    bucket='my-versioned-assets')

with open(vc('/foo/bar.json'), 'r') as f:
    data = json.load(f)
```

Or, with [`baiji-serialization`][baiji-serialization]:

```
from baiji.serialization import json
data = json.load(vc('s3://example-bucket/example.json'))
```

To add a new versioned path, or update an existing one, use the `vc`
command-line tool:

```
vc add /foo/bar.csv ~/Desktop/bar.csv
vc update --major /foo/bar.csv ~/Desktop/new_bar.csv
vc update --minor /foo/bar.csv ~/Desktop/new_bar.csv
vc update --patch /foo/bar.csv ~/Desktop/new_bar.csv
```

A VersionedCache object is specific to a manifest file and a bucket.

Though the version number uses semver-like semantics, the cache ignores
version semantics. The manifest pins an exact version number.


### The asset cache

The asset cache works at a lower level of abstraction. It holds local copies
of arbitrary S3 assets. Calling the `cache()` function with an S3 path ensures
that the file is available locally, and then returns a valid, local path.

On a cache miss, the file is downloaded to the cache and then its local path
is returned. Subsequent calls will return the same local path. After a
timeout, which defaults to one day, the validity of the local file is checked
by comparing a local MD5 hash with the remote etag. This check is repeated
once per day.

To gain a performance boost, you can configure immutable buckets, whose
contents are never revalidated after download. The versioned cache uses this
feature.

```
import json
from baiji.pod import AssetCache

cache = AssetCache.create_default()

with open(cache('s3://example-bucket/example.json'), 'r') as f:
    data = json.load(f)
```

Or, with [`baiji-serialization`][baiji-serialization]:

```
from baiji.serialization import json
data = json.load(cache('s3://example-bucket/example.json'))
```

It is safe to call `cache` multiple times: `cache(cache('path'))` will behave
correctly.

[baiji-serialization]: https://github.com/bodylabs/baiji-serialization


Tips
----

When you're developing, you often want to try out variations on a file before
committing to a particular one. Rather than incrementing the patch level over
and over, you can set `manifest.json` to include an absolute path:

```
    "/foo/bar.csv": "/Users/me/Desktop/foo.obj",
```

This can be either a local or an s3 path; use local if you're iterating by
yourself, and s3 to iterate with other developers or in CI.


Development
-----------

```sh
pip install -r requirements_dev.txt
rake unittest
rake lint
```


TODO
----

- Add vc config to config
    - Explain or clean up the weird default_bucket config logic in
      prefill_runner. e.g. This logic is so that we can have a customized
      script in core that doesn't require these arguments.
- Use config without subclassing. Pass overries to init
- Configure using an importable config path instead of injecting. Or, possibly,
  allow ~/.aws/baiji_config to change defaults.
- Rework baiji.pod.util.reachability and perhaps baiji.util.reachability
  as well.
- Restore CDN publish functionality in core
- Avoid using actual versioned assets. Perhaps write some (smaller!)
  files to a test bucket and use those?
- Remove suffixes support in vc.uri, used only for CDNPublisher
- Move yaml.dump and json.* to baiji. Possibly do a
  `try: from baiji.serialization.json import load, dump; except ImportError: def load(...`
   Or at least have a comment to the effect of "don't use this, use baiji.serialization.json"
- Use consistent argparse pattern in the runners.
- I think it would be better if the CacheFile didn't need to know about the
  AssetCache, to avoid this bi-directional dependency. It's only required in
  the constructor, but that could live on the AssetCache, e.g.
  create_cache_file(path, bucket=None).


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
