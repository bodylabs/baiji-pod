class CacheUtilRunner(object):
    def __init__(self, asset_cache):
        self.cache = asset_cache

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(
            description='baiji-pod asset cache utility',
            epilog='keys are a kind of URL, of the form s3://BUCKET/PATH/TO/FILE')
        commands = parser.add_subparsers(help='sub-command help', dest='command')

        cache_command = commands.add_parser(
            'cache', help='cache a file')
        cache_command.add_argument(
            'key', type=str, help='key to cache: s3://BUCKET/PATH/TO/FILE')
        cache_command.add_argument(
            '-u', '--update', action='store_true', help='always check for updates')

        del_command = parser.subs.add_parser(
            'del', help='remove a file from the cache')
        del_command.add_argument(
            'key', type=str, help='key to delete: s3://BUCKET/PATH/TO/FILE')

        ls_command = parser.subs.add_parser(
            'ls', help='list everything in the cache')
        ls_command.add_argument(
            '-l', '--details', action='store_true', help='more detail')

        parser.subs.add_parser(
            'loc', help='print the location of the cache')

        return parser.parse_args()

    def main(self):
        from baiji.pod.util.format_bytes import format_bytes

        args = self._parse_args()

        if args.command == 'cache':
            self.cache(args.key, force_check=args.update)

        elif args.command == 'del':
            self.cache.delete(args.key)

        elif args.command == 'ls':
            if args.details:
                for x in self.cache.ls():
                    outdated = 'outdated ' if x.is_outdated else ''
                    print '{remote_uri} {file_size} {outdated}{age:.0f} days'.format(
                        remote_uri=x.remote,
                        file_size=format_bytes(x.size),
                        outdated=outdated,
                        age=x.age/(60*60*24)
                    ).encode('utf-8')
            else:
                print u'\n'.join([x.remote for x in self.cache.ls()]).encode('utf-8')

        elif args.command == 'loc':
            print self.cache.config.cache_dir

        # On success, exit with status code of 0.
        return 0
