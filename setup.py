from setuptools import setup, find_packages

setup(name='greencap',
      version='0.1',
      description='GREENCap: Asynchronous requests and middleware API for REDCap.',
      url='https://github.com/tshanebuckley/GREENCap',
      author='Maintainer: Shane Buckley',
      author_email='buckleyts3@email.unc.edu',
      #license='MIT',
      python_requires='>=3.6',
      #package_dir={'': 'GREENCap/greencap'},
      packages=['greencap', 'GREENCap/utils', 'GREENCap/api'],
      include_package_data=True,
      #packages=find_packages(),
      install_requires=[],
      zip_safe=False
      )