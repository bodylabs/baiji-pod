class SCRunner(object):
    def __init__(self, static_cache):
        self.sc = static_cache

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(
            description='baiji-pod static cache',
            epilog='keys are a kind of URL, of the form s3://BUCKET/PATH/TO/FILE')
        parser.subs = parser.add_subparsers(help='sub-command help', dest='command')
        subparsers = {}

        subparsers['cache'] = parser.subs.add_parser('cache', help='cache a file')
        subparsers['del'] = parser.subs.add_parser('del', help='remove a file from the cache')
        subparsers['prefill'] = parser.subs.add_parser('prefill', help='pre-download the usual files')
        subparsers['ls'] = parser.subs.add_parser('ls', help='list everything in the cache')
        subparsers['loc'] = parser.subs.add_parser('loc', help='print the location of the cache')
        subparsers['pack'] = parser.subs.add_parser('pack', help='print the location of the cache')
        subparsers['unpack'] = parser.subs.add_parser('unpack', help='print the location of the cache')

        subparsers['cache'].add_argument('key', type=str, help='key to cache: s3://BUCKET/PATH/TO/FILE')
        subparsers['cache'].add_argument('-u', '--update', action='store_true', help="always check for updates")

        subparsers['del'].add_argument('key', type=str, help='key to delete: s3://BUCKET/PATH/TO/FILE')

        subparsers['prefill'].add_argument('-f', '--file', default=None, help='YAML file containing what to prefill')
        subparsers['prefill'].add_argument('-v', '--verbose', action='store_true', default=False, help='print verbose info such as which file are getting pre-filled')

        subparsers['ls'].add_argument('-l', '--details', action='store_true', help='more detail')

        subparsers['pack'].add_argument('manifest', type=str, default=None, help='File listing sc paths to package')
        subparsers['pack'].add_argument('save_to', type=str, default=None, help='Location to save the package')
        subparsers['pack'].add_argument('--max_size', type=int, default=None, help='max size of the packaged zip files, in MB')

        subparsers['unpack'].add_argument('files', type=str, nargs='+', help='zip files to unpack into the sc cache')

        return parser.parse_args()

    def main(self):
        import os

        args = self._parse_args()

        if args.command == 'cache':
            self.sc(args.key, force_check=args.update)

        elif args.command == 'del':
            self.sc.delete(args.key)

        elif args.command == 'prefill':
            if args.file is not None:
                args.file = os.path.expanduser(args.file)
            self.sc.prefill(args.file, args.verbose)

        elif args.command == 'ls':
            if args.details:
                from bodylabs.util.numerics import sizeof_format_human_readable
                for x in self.sc.ls():
                    outdated = 'outdated ' if x.is_outdated else ''
                    print '{is_remote} {file_size} {outdated}{age:.0f} days'.format(
                        is_remote=x.remote,
                        file_size=sizeof_format_human_readable(x.size),
                        outdated=outdated,
                        age=x.age/(60*60*24)
                    ).encode('utf-8')
            else:
                print u"\n".join([x.remote for x in self.sc.ls()]).encode('utf-8')

        elif args.command == 'loc':
            print self.sc.config.cache_dir

        elif args.command == 'pack':
            self.sc.pack(args.manifest, args.save_to, max_size=args.max_size)

        elif args.command == 'unpack':
            self.sc.unpack(args.files)

        # On success, exit with status code of 1.
        return 1
