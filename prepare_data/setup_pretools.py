from distutils.core import setup, Extension
from Cython.Build import cythonize


setup(
	name = 'pre-tools',
    ext_modules=cythonize("pre_tools.pyx"),
) 

