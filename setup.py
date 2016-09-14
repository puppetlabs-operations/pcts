from setuptools import setup, find_packages

__version__ = None

with open('pcts/version.py') as f:
    exec(f.read())

setup(
    name='pcts',
    version=__version__,
    py_modules=['pcts'],
    packages=find_packages(),
    include_package_data=True,
    author='Puppet SysOps Team',
    author_email='sysops-dept@puppet.com',
    url='https://github.com/puppetlabs-operations/pcts',
    license='Apache License 2.0',
    install_requires=[
        'aiohttp',
        'elasticsearch',
        'pygithub',
        'python-systemd==231',
    ],
    dependency_links=['https://github.com/systemd/python-systemd/tarball/v231#egg=python-systemd-231'],
    python_requires='>= 3.4',
    entry_points='''
        [console_scripts]
        pcts-service=pcts.__main__.main()
    ''',
)
