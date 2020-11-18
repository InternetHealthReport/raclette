from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

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
  cmdclass = {"build_ext": build_ext},
  ext_modules = ext_modules)

