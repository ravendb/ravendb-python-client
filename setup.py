from setuptools import setup, find_packages

setup(
    name='pyravendbV4.0',
    packages=find_packages(),
    version='4.0.1.0',
    description='This is the official python client for RavenDB v4.0 document database, this is Alpha version',
    author='Idan Haim Shalom',
    author_email='haimdude@gmail.com',
    url='https://github.com/IdanHaim/RavenDB-Python-Client',
    license='MIT',
    keywords='pyravendb',
    install_requires=
    [
        "pysodium >= 0.6.9.1",
        "requests >= 2.13.0",
        "inflector >= 2.0.11",
    ],
    zip_safe=False
)
