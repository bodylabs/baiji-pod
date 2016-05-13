class PrefillRunner(object):
    def __init__(self, static_cache, default_vc_manifest_path=None, default_vc_bucket=None):
        self.sc = static_cache
        self.default_vc_manifest_path = default_vc_manifest_path
        self.default_vc_bucket = default_vc_bucket

    def _create_vc(self, manifest_path=None, bucket=None):
        from baiji.pod import VersionedCache

        if manifest_path is None:
            manifest_path = self.default_vc_manifest_path

        if bucket is None:
            bucket = self.default_vc_bucket

        return VersionedCache(
            static_cache=self.sc,
            manifest_path=manifest_path,
            bucket=bucket)

    def _parse_args(self):
        import argparse

        parser = argparse.ArgumentParser(description='baiji-pod prefill tool')

        if self.default_vc_bucket is None:
            parser.add_argument('--vc_bucket', required=True, type=str, help='S3 bucket name for VC paths')
        else:
            parser.add_argument('--vc_bucket', default=None, type=str, help='S3 bucket name for VC paths; defaults to {}'.format(self.default_vc_bucket))

        if self.default_vc_manifest_path is None:
            parser.add_argument('--vc_manifest', required=True, type=str, help='S3 VC manifest')
        else:
            parser.add_argument('--vc_manifest', default=None, type=str, help='S3 VC manifest; defaults to {}'.format(self.default_vc_manifest_path))

        parser.add_argument('-f', '--file', required=True, help='YAML file containing what to prefill')
        parser.add_argument(
            '-v', '--verbose', action='store_true', default=False,
            help='print verbose info such as which file are getting pre-filled')

        return parser.parse_args()

    def main(self):
        import os
        from baiji.pod.prefill import prefill
        from baiji.pod.util import yaml

        args = self._parse_args()

        vc = self._create_vc(manifest_path=args.vc_manifest, bucket=args.vc_bucket)

        paths = yaml.load(os.path.expanduser(args.file))

        if paths is None:
            print 'Nothing to prefill!'
        else:
            prefill(
                static_cache=self.sc,
                versioned_cache=vc,
                paths=paths,
                verbose=args.verbose)

        # On success, exit with status code of 1.
        return 1
