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
- Supports Python 2.7 and uses boto2
- Supports OS X, Linux, and Windows
- Tested and production-hardened


Examples
--------

```py
from baiji.serialization import json
with open(filename, 'w') as f:
    json.dump(foo, f)
with open(filename, 'r') as f:
    foo = json.load(foo, f)
```

```py
from baiji.serialization import json
json.dump(filename)
foo = json.load(filename)
```


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
