def format_bytes(bytes):
    '''
    Sweet little implementation from
    http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    '''
    for x in ['b', 'kb', 'mb', 'gb']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'tb')
