from setuptools import setup, find_packages

setup(
    name='pyravendb',
    packages=find_packages(),
    version='1.3.0',
    description='This is the official python client for RavenDB document database',
    author='Idan Haim Shalom',
    author_email='haimdude@gmail.com',
    url='https://github.com/IdanHaim/RavenDB-Python-Client',
    license='GNU',
    keywords='pyravendb',
    install_requires=
    [
        "pycrypto >= 2.6.1",
        "requests >= 2.9.1",
        "inflector >= 2.0.11",
        "enum >= 0.4.6",
    ],
    zip_safe=False
)
