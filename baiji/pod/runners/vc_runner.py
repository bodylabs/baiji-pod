class VCRunner(object):
    def __init__(self, cache, default_manifest_path=None, default_bucket=None):
        self.cache = cache
        self.default_manifest_path = default_manifest_path
        self.default_bucket = default_bucket

    def _create_vc(self, manifest_path=None, bucket=None):
        from baiji.pod import VersionedCache

        if manifest_path is None:
            manifest_path = self.default_manifest_path

        if bucket is None:
            bucket = self.default_bucket

        return VersionedCache(
            cache=self.cache,
            manifest_path=manifest_path,
            bucket=bucket)

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(
            description='baiji-pod versioned cache tool',
            epilog='paths are within a particular bucket, so use / rooted paths')

        if self.default_bucket is None:
            parser.add_argument('--bucket', required=True, type=str, help='S3 bucket name')
        else:
            parser.add_argument(
                '--bucket', default=None, type=str,
                help='S3 bucket name; defaults to {}'.format(self.default_bucket))

        if self.default_manifest_path is None:
            parser.add_argument('--manifest', required=True, type=str, help='Version manifest')
        else:
            parser.add_argument(
                '--manifest', default=None, type=str,
                help='Version manifest; defaults to {}'.format(self.default_manifest_path))

        parser.subs = parser.add_subparsers(help='sub-command help', dest='command')
        subparsers = {}

        subparsers['add'] = parser.subs.add_parser('add', help='start versioning a file')
        subparsers['update'] = parser.subs.add_parser(
            'update', help='update a versioned file')
        subparsers['versions'] = parser.subs.add_parser(
            'versions', help='list versions available for a versioned file')
        subparsers['sync'] = parser.subs.add_parser(
            'sync', help='sync a manifest to a local directory')
        subparsers['ls'] = parser.subs.add_parser(
            'ls', help='list versioned files in the manifest')
        subparsers['ls-remote'] = parser.subs.add_parser(
            'ls-remote', help='list versioned files in the storage bucket')
        subparsers['get'] = parser.subs.add_parser('get', help='download a file')
        subparsers['path'] = parser.subs.add_parser(
            'path', help='cache the file locally and output its path ' +
            '(e.g. open `vc path /foo/bar.png`)')
        subparsers['open'] = parser.subs.add_parser(
            'open', help='cache the file locally and open it using /usr/bin/open')
        subparsers['path-remote'] = parser.subs.add_parser(
            'path-remote', help="output the file's remote path")
        subparsers['cat'] = parser.subs.add_parser(
            'cat', help='write the contents of a file to stdout')

        subparsers['add'].add_argument('path', type=str, help='path to store the file at')
        subparsers['add'].add_argument(
            'file', type=str, help='file to cache (may be local or on s3)')

        subparsers['update'].add_argument(
            'path', type=str, help='path to store the file at')
        subparsers['update'].add_argument(
            'file', type=str, help='file to cache (may be local or on s3)')
        subparsers['update'].add_argument(
            '--major', default=False, action='store_true', help='This is a major update')
        subparsers['update'].add_argument(
            '--minor', default=False, action='store_true', help='This is a minor update')
        subparsers['update'].add_argument(
            '--patch', default=False, action='store_true', help='This is a patch update')

        subparsers['versions'].add_argument('path', type=str, help='path to list versions for')

        subparsers['sync'].add_argument(
            'destination', nargs='?', default='./versioned_assets', type=str,
            help='path to sync the manifest to (default is ./versioned_assets)')

        subparsers['get'].add_argument('path', type=str, help='path to get')
        subparsers['get'].add_argument('version', type=str, nargs='?', help='version to get')
        subparsers['get'].add_argument('destination', type=str, help='path to write the file to')

        subparsers['path'].add_argument('path', type=str, help='path to get')
        subparsers['path'].add_argument('version', type=str, nargs='?', help='version to get')

        subparsers['open'].add_argument('path', type=str, help='path to get')
        subparsers['open'].add_argument('version', type=str, nargs='?', help='version to get')

        subparsers['path-remote'].add_argument('path', type=str, help='path to get')
        subparsers['path-remote'].add_argument(
            'version', type=str, nargs='?', help='version to get')

        subparsers['cat'].add_argument('path', type=str, help='path to cat')
        subparsers['cat'].add_argument('version', type=str, nargs='?', help='version to cat')

        return parser.parse_args()

    def main(self):
        from baiji import s3

        args = self._parse_args()

        vc = self._create_vc(manifest_path=args.manifest, bucket=args.bucket)

        if args.command == 'add':
            vc.add(args.path, args.file, verbose=True)

        if args.command == 'update':
            vc.update(
                args.path,
                args.file,
                major=args.major,
                minor=args.minor,
                patch=args.patch,
                verbose=True)

        if args.command == 'versions':
            for v in vc.versions_available(args.path):
                print v

        if args.command == 'sync':
            print 'sync to {}'.format(args.destination)
            vc.sync(args.destination)

        if args.command == 'ls':
            print '\n'.join(sorted(vc.manifest_files))

        if args.command == 'ls-remote':
            print '\n'.join(sorted(vc.ls_remote()))

        if args.command == 'get':
            f = vc(args.path, version=args.version)
            print 'copying {} version {} to {}'.format(
                args.path,
                vc.manifest_version(args.path),
                args.destination)
            s3.cp(f, args.destination)

        if args.command == 'path':
            print vc(args.path, version=args.version)

        if args.command == 'open':
            import subprocess
            subprocess.call(['open', vc(args.path, version=args.version)])

        if args.command == 'path-remote':
            print vc.uri(args.path, version=args.version)

        if args.command == 'cat':
            import shutil
            import sys
            f = vc(args.path, version=args.version)
            shutil.copyfileobj(open(f, 'rb'), sys.stdout)

        # On success, exit with status code of 0.
        return 0
