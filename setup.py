from setuptools import setup, find_packages

setup(
    name='pyravendb',
    packages=find_packages(),
    version='4.0.6.2',
    long_description=open("README.rst").read(),
    description='This is the official python client for RavenDB v4.0 document database',
    author='RavenDB',
    author_email='support@ravendb.net',
    url='https://github.com/ravendb/RavenDB-Python-Client',
    license='MIT',
    keywords='pyravendb',
    install_requires=
    [
        'requests >= 2.18.4',
        'xxhash >= 1.0.1',
        'pyOpenSSL >= 17.2.0',
        'ijson == 2.3',
        'websocket-client >= 0.46.0',
        'inflect >= 1.0.0'
    ],
    zip_safe=False
)
