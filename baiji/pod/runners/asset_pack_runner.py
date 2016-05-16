class AssetPackRunner(object):
    def __init__(self,
                 cache,
                 default_vc_manifest_path=None,
                 default_vc_bucket=None):
        self.cache = cache
        self.default_vc_manifest_path = default_vc_manifest_path
        self.default_vc_bucket = default_vc_bucket

    def _create_vc(self, manifest_path=None, bucket=None):
        from baiji.pod import VersionedCache

        if manifest_path is None:
            manifest_path = self.default_vc_manifest_path

        if bucket is None:
            bucket = self.default_vc_bucket

        return VersionedCache(
            cache=self.cache,
            manifest_path=manifest_path,
            bucket=bucket)

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(description='baiji-pod pack tool')
        parser.subs = parser.add_subparsers(help='sub-command help', dest='command')
        subparsers = {}

        subparsers['dump'] = parser.subs.add_parser('dump', help='')
        subparsers['load'] = parser.subs.add_parser('load', help='')

        if self.default_vc_bucket is None:
            subparsers['dump'].add_argument(
                '--vc_bucket', required=True, type=str,
                help='S3 bucket name for VC paths')
        else:
            subparsers['dump'].add_argument(
                '--vc_bucket', default=None, type=str,
                help='S3 bucket name for VC paths; defaults to {}'.format(
                    self.default_vc_bucket))

        if self.default_vc_manifest_path is None:
            subparsers['dump'].add_argument(
                '--vc_manifest',
                required=True, type=str, help='S3 VC manifest')
        else:
            subparsers['dump'].add_argument(
                '--vc_manifest', default=None, type=str,
                help='S3 VC manifest; defaults to {}'.format(
                    self.default_vc_manifest_path))

        subparsers['dump'].add_argument(
            'file', help='YAML file containing what to pack')
        subparsers['dump'].add_argument(
            'save_to', type=str, default=None,
            help='Location to save the package')
        subparsers['dump'].add_argument(
            '--max_size', type=int, default=None,
            help='max size of the packaged zip files, in MB')

        subparsers['load'].add_argument(
            'files', type=str, nargs='+',
            help='zip files to unpack into the sc cache')

        return parser.parse_args()

    def main(self):
        import os
        from baiji.pod import asset_pack
        from baiji.pod.util import yaml

        args = self._parse_args()

        if args.command == 'dump':
            vc = self._create_vc(
                manifest_path=args.vc_manifest,
                bucket=args.vc_bucket)
            paths = yaml.load(os.path.expanduser(args.file))
            asset_pack.dump(self.cache, vc, paths, args.save_to, max_size=args.max_size)

        elif args.command == 'load':
            asset_pack.load(self.cache, args.files)

        # On success, exit with status code of 0.
        return 0
