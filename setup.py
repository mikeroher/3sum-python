from distutils.core import setup
from Cython.Build import cythonize

setup(
    name = "3sum",
    ext_modules = cythonize('./main.pyx'),  # accepts a glob pattern
)