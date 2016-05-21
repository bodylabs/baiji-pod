TODO
====

- Add vc config to config
- Use config without subclassing. Pass overries to init
- Configure using an importable config path instead of injecting. Or, possibly,
  allow ~/.aws/baiji_config to change defaults.
- Rework baiji.pod.util.reachability and perhaps baiji.util.reachability
  as well.
- Restore CDN publish functionality in core
- Avoid using actual versioned assets. Perhaps write some (smaller!)
  files to a test bucket and use those?
- Remove suffixes support in vc.uri, used only for CDNPublisher
