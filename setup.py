from distutils.core import setup
from Cython.Build import cythonize

setup(name='raclette',
      ext_modules=cythonize("src/tracksaggregator_cy.pyx"))
