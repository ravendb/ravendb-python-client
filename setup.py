from setuptools import setup, find_packages

setup(
    name='pyravendb',
    packages=find_packages(),
    version='4.0.4.2',
    description='This is the official python client for RavenDB v4.0 document database',
    author='Idan Haim Shalom',
    author_email='haimdude@gmail.com',
    url='https://github.com/ravendb/RavenDB-Python-Client',
    license='MIT',
    keywords='pyravendb',
    install_requires=
    [
        'requests >= 2.18.4',
        'inflector >= 2.0.12',
        'xxhash >= 1.0.1',
        'pyOpenSSL >= 17.2.0',
        'ijson >= 2.3',
        'websocket-client >= 0.46.0'
    ],
    zip_safe=False
)
