try:
    from setuputils import setup
except:
    from distutils.core import setup


    
setup(
    name = 'iointegrity',
    version = '0.0.1',
    description = 'libraries and tools to check iointegrity',
    author = 'stevec7',
    author_email = 'none',
    #packages = ['iointegrity', 'mpi4py', 'sqlite3']
    packages = ['iointegrity']






)
