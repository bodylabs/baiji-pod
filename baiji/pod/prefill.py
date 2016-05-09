from baiji.util.parallel import ParallelWorker


class PrefillWorker(ParallelWorker):
    def __init__(self, cache_obj, verbose=False):
        from baiji.pod.versioned import VersionedCache
        self.sc = cache_obj
        self.vc = VersionedCache(cache=self.sc)
        self.verbose = verbose
        super(PrefillWorker, self).__init__()

    def on_run(self, remote):
        import sys
        from baiji import s3

        if not self.verbose:
            sys.stdout.write('.')

        try:
            if remote.startswith("s3://"):
                self.sc(remote, verbose=self.verbose)
            else:
                self.vc(remote, verbose=self.verbose)
        except s3.KeyNotFound:
            print "{} is in the prefill manifest, but is not found!".format(remote)


def prefill(self, prefill_file=None, verbose=False):
    '''
    By default, prefill uses the list of assets checked in to core at
    bodylabs/cache/sc_gc.conf.yaml. To prefill with an alternate list
    of files, use `prefill_file`.
    '''
    from baiji.util.parallel import parallel_for
    from bodylabs.util.timer import Timer

    with Timer(verbose=False) as t:
        parallel_for(SCExemptList(prefill_file), PrefillWorker, args=[self, verbose], num_processes=12)

    print ''
    print 'sc prefill done in {} seconds'.format(t.elapsed_time_s)
