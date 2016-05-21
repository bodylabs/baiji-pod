# This version number is stored here instead of in `__init__.py` because
# it gets imported by `setup.py` before dependencies are installed, and
# some of the imports in `__init__.py` require dependencies to be loaded.
#
# Observed when installing in CI, where cached_property is not installed
# at the outset.

__version__ = '1.0.0'
