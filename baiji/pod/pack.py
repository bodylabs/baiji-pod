def pack(self, manifest, save_to, max_size=None):
    import zipfile
    from bodylabs.cache.versioned import VersionedCache
    from bodylabs.serialization import multiformat
    vc = VersionedCache(cache=self)

    class FileToPack(object):
        def __init__(self, src):
            self.uri = src
            parsed_src = s3.path.parse(src)
            if parsed_src[1] in sc_conf.immutable_buckets and vc.is_versioned(parsed_src[2]):
                self.src = vc(parsed_src[2])
            else:
                self.src = sc(src)
            self.dst = self.src.replace(sc_conf.cache_dir, '')
            if self.dst.startswith('/'):
                self.dst = self.dst[1:]
        def __repr__(self):
            return "<sc pack %s>" % (self.uri, )
        @property
        def size(self):
            try:
                return self._size
            except AttributeError:
                self._size = os.stat(self.src).st_size # FIXME pylint: disable=attribute-defined-outside-init
                return self._size

    files_to_pack = sorted([FileToPack(f) for f in multiformat.load(manifest)], key=lambda x: x.size, reverse=True)
    if max_size is not None:
        mb = 1024 * 1024
        max_size = max_size * mb
        largest_file_size = max([x.size for x in files_to_pack])
        if max_size < largest_file_size:
            raise ValueError("max size allowed is %d mb but there's a file of %d mb; no can do" % (max_size/mb, largest_file_size/mb))
        zip_files = []
        for f in files_to_pack:
            # Doing this optimally is reducible to knapsack, but we'll just do the simple, greedy thing here. This will be run few enough times that it's not worth getting clever.
            def first_fit(zip_files, size):
                for ii, zf in enumerate(zip_files):
                    if size < max_size - sum([x.size for x in zf]):
                        return ii
            ii = first_fit(zip_files, f.size)
            if ii is None:
                zip_files.append([f])
            else:
                zip_files[ii].append(f)
    else:
        zip_files = [files_to_pack]
    for ii, files_in_zip in enumerate(zip_files):
        if max_size is not None:
            zip_path = os.path.splitext(save_to)[0] + '_%d.zip' % (ii+1)
        else:
            zip_path = os.path.splitext(save_to)[0] + '.zip'
        print "Building", zip_path, "with", len(files_in_zip), "files,", sum([x.size for x in files_in_zip]), "bytes"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for f in files_in_zip:
                print "  Adding", f.dst
                zf.write(f.src, f.dst)

def unpack(self, files):
    import zipfile
    for f in files:
        with zipfile.ZipFile(f, 'r') as zf:
            zf.extractall(sc_conf.cache_dir)
