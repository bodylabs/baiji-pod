from __future__ import absolute_import

from baiji.util.yaml import load

def dump(obj, path):
    '''
    from baiji.pod.util import yaml
    obj = {'foo': 123}
    yaml.dump(obj, 'foo.yml')
    '''
    with open(path, 'w') as f:
        yaml.dump(obj, f, default_flow_style=False)
