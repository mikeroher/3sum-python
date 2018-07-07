from distutils.core import setup
from distutils.extension import Extension
import sys

if '--use-cython' in sys.argv:
    USE_CYTHON = True
    sys.argv.remove('--use-cython')
else:
    USE_CYTHON = False
ext = '.pyx' if USE_CYTHON else '.cpp'
extensions = [Extension("threesum",
                        ["main"+ext],
                        language='c++',
                        include_dirs=['./'])]

if USE_CYTHON:
    from Cython.Build import cythonize
    extensions = cythonize(extensions)

setup(
    ext_modules = extensions
)