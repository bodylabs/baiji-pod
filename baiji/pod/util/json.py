from __future__ import absolute_import

def load(path):
    '''
    from baiji.pod.util import json
    foo = json.load('foo.json')
    '''
    import json
    with open(path, 'r') as f:
        return json.load(f)
