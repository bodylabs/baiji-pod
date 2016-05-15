class SCRunner(object):
    def __init__(self, static_cache):
        self.sc = static_cache

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(
            description='baiji-pod asset cache utility',
            epilog='keys are a kind of URL, of the form s3://BUCKET/PATH/TO/FILE')
        parser.subs = parser.add_subparsers(help='sub-command help', dest='command')
        subparsers = {}

        subparsers['cache'] = parser.subs.add_parser('cache', help='cache a file')
        subparsers['del'] = parser.subs.add_parser(
            'del', help='remove a file from the cache')
        subparsers['ls'] = parser.subs.add_parser(
            'ls', help='list everything in the cache')
        subparsers['loc'] = parser.subs.add_parser(
            'loc', help='print the location of the cache')

        subparsers['cache'].add_argument(
            'key', type=str, help='key to cache: s3://BUCKET/PATH/TO/FILE')
        subparsers['cache'].add_argument(
            '-u', '--update', action='store_true', help='always check for updates')

        subparsers['del'].add_argument(
            'key', type=str, help='key to delete: s3://BUCKET/PATH/TO/FILE')

        subparsers['ls'].add_argument(
            '-l', '--details', action='store_true', help='more detail')

        return parser.parse_args()

    def main(self):
        from baiji.pod.util.format_bytes import format_bytes

        args = self._parse_args()

        if args.command == 'cache':
            self.sc(args.key, force_check=args.update)

        elif args.command == 'del':
            self.sc.delete(args.key)

        elif args.command == 'ls':
            if args.details:
                for x in self.sc.ls():
                    outdated = 'outdated ' if x.is_outdated else ''
                    print '{is_remote} {file_size} {outdated}{age:.0f} days'.format(
                        is_remote=x.remote,
                        file_size=format_bytes(x.size),
                        outdated=outdated,
                        age=x.age/(60*60*24)
                    ).encode('utf-8')
            else:
                print u'\n'.join([x.remote for x in self.sc.ls()]).encode('utf-8')

        elif args.command == 'loc':
            print self.sc.config.cache_dir

        # On success, exit with status code of 0.
        return 0
