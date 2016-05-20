def remove_tree(path, ignore_errors=True):
    import os
    import shutil
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=ignore_errors)
    else:
        remove_file(path, ignore_errors)

def remove_file(path, ignore_errors=True):
    import os
    try:
        os.remove(path)
    except OSError as e:
        if e.errno == 2: # File not found
            if not ignore_errors:
                raise
        else: # Something else, like permission denied
            raise
