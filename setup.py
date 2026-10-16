from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize

ext_modules=[
        Extension("raclette.tracksaggregator_cy",
              ["raclette/tracksaggregator_cy.pyx"],
              libraries=["m"],
              extra_compile_args = ["-ffast-math", "-fopenmp"],
              extra_link_args=['-fopenmp'],
              language="c++",
              ),
              
        Extension("raclette.timetrack.allin_cy",
              ["raclette/timetrack/allin_cy.pyx"],
              libraries=["m"],
              extra_compile_args = ["-ffast-math"],
              ),
              
        Extension("raclette.timetrack.lastmile",
              ["raclette/timetrack/lastmile.pyx"],
              libraries=["m"],
              extra_compile_args = ["-ffast-math"],
              ),
              
              
              ]

setup(
  name = "raclette",
  ext_modules = cythonize(ext_modules, language_level="3"))

