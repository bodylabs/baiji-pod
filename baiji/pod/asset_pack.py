def dump(cache, versioned_cache, paths, save_to, max_size=None):
    '''
    Create an asset pack: a series of zip files containing the specified sc and
    vc assets.

    paths: A list of s3 paths, which are handled by sc, and versioned paths,
      which are handled by vc.
    save_to: The path of a zipfile to write.
    max_size: The maximum size of the zip file, in bytes. When the asset pack
      is larger than this, it will be broken into multiple zip files.
    '''
    import os
    import zipfile
    from cached_property import cached_property

    vc = versioned_cache

    class FileToPack(object):
        def __init__(self, src):
            from baiji import s3

            self.cache = cache
            self.uri = src

            parsed_src = s3.path.parse(src)
            if parsed_src[1] in cache.config.immutable_buckets and \
                vc.is_versioned(parsed_src[2]):
                self.src = vc(parsed_src[2])
            else:
                self.src = cache(src)

            self.dst = self.src.replace(cache.config.cache_dir, '')
            if self.dst.startswith('/'):
                self.dst = self.dst[1:]

        def __repr__(self):
            return '<sc pack {}>'.format(self.uri)

        @cached_property
        def size(self):
            return os.stat(self.src).st_size

    files_to_pack = sorted(
        [FileToPack(f) for f in paths],
        key=lambda x: x.size,
        reverse=True)

    if max_size is not None:
        mb = 1024 * 1024
        max_size = max_size * mb
        largest_file_size = max([x.size for x in files_to_pack])
        if max_size < largest_file_size:
            raise ValueError(
                ("max size allowed is %d mb but there's a file of %d mb; " +
                 "no can do") % (max_size/mb, largest_file_size/mb))
        zip_files = []
        for f in files_to_pack:
            # Doing this optimally is reducible to knapsack, but we'll just do
            # the simple, greedy thing here. This will be run few enough times
            # that it's not worth getting clever.
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

        print 'Building {} with {} files, {} bytes'.format(
            zip_path, len(files_in_zip), sum([x.size for x in files_in_zip]))

        with zipfile.ZipFile(
            zip_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for f in files_in_zip:
                print '  Adding {}'.format(f.dst)
                zf.write(f.src, f.dst)

def load(static_cache, asset_pack_paths):
    import zipfile
    for asset_path_pack in asset_pack_paths:
        with zipfile.ZipFile(asset_path_pack, 'r') as zf:
            zf.extractall(static_cache.config.cache_dir)
