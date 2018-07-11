from distutils.core import setup, Extension
from Cython.Build import cythonize
import numpy

setup(
    name = "3sum",
    ext_modules = cythonize('./threesum.pyx'),  # accepts a glob pattern
    include_dirs=[numpy.get_include()]
)