TODO
====

- Add vc config to config
    - Explain or clean up the weird default_bucket config logic in
      prefill_runner. e.g. This logic is so that we can have a customized
      script in core that doesn't require these arguments.
- Use config without subclassing. Pass overries to init
- Configure using an importable config path instead of injecting. Or, possibly,
  allow ~/.aws/baiji_config to change defaults.
- Rework baiji.pod.util.reachability and perhaps baiji.util.reachability
  as well.
- Restore CDN publish functionality in core
- Avoid using actual versioned assets. Perhaps write some (smaller!)
  files to a test bucket and use those?
- Remove suffixes support in vc.uri, used only for CDNPublisher
- Move yaml.dump and json.* to baiji. Possibly do a
  `try: from baiji.serialization.json import load, dump; except ImportError: def load(...`
   Or at least have a comment to the effect of "don't use this, use baiji.serialization.json"
