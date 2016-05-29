from baiji.util.parallel import ParallelWorker


class PrefillWorker(ParallelWorker):
    def __init__(self, asset_cache, versioned_cache, verbose=False):
        super(PrefillWorker, self).__init__()
        self.cache = asset_cache
        self.vc = versioned_cache
        self.verbose = verbose

    def on_run(self, remote):
        import sys
        from baiji import s3

        if not self.verbose:
            sys.stdout.write('.')

        try:
            if remote.startswith('s3://'):
                self.cache(remote, verbose=self.verbose)
            else:
                self.vc(remote, verbose=self.verbose)
        except s3.KeyNotFound:
            print '{} is in the prefill manifest, but is not found!'.format(remote)


def prefill(asset_cache, versioned_cache, paths, num_processes=None, verbose=False):
    from baiji.util.parallel import parallel_for
    from harrison import Timer

    if num_processes is None:
        num_processes = asset_cache.config.num_prefill_processes

    with Timer(verbose=False) as t:
        parallel_for(
            paths,
            PrefillWorker,
            args=[asset_cache, versioned_cache, verbose],
            num_processes=num_processes)

    print ''
    print 'sc prefill done in {} seconds'.format(t.elapsed_time_s)
