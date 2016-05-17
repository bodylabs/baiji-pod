# Publish into the `baiji` namespace.
#
# http://stackoverflow.com/a/1676069/893113
# https://www.python.org/dev/peps/pep-0420/

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
