'''
Special-casing EC2 using specific environment-variable conventions seems like
something which is too specific for this library, belonging in configuration
instead.

It seems that baiji's approach of checking for an internet connection before
each request is appropriate for development, but not for production. Perhaps
that also should be reworked a bit, or handled in configuration, etc.
'''

from env_flag import env_flag

def location_is_ec2():
    """
    Return True if running on EC2.

    For this check to work, you need to be running within supervisord or
    bin/with_ec2_env.
    """
    return env_flag('EC2', False)

class InternetUnreachableError(Exception):
    pass

def assert_internet_reachable():
    from baiji.util.reachability import internet_reachable

    # ec2 is the internet.
    if location_is_ec2():
        return

    if not internet_reachable():
        raise InternetUnreachableError('Internet Unreachable')
