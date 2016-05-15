def format_bytes(num_bytes):
    '''
    Sweet little implementation from
    http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    '''
    for unit in ['b', 'kb', 'mb', 'gb']:
        if num_bytes < 1024.0 and num_bytes > -1024.0:
            return "%3.1f%s" % (num_bytes, unit)
        num_bytes /= 1024.0
    return "%3.1f%s" % (num_bytes, 'tb')
