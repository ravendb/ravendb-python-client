from setuptools import setup, find_packages

setup(
    name='pyravendb',
    packages=find_packages(),
    version='1.0.3',
    description='This is the official python client for RavenDB document database',
    author='Idan Haim Shalom',
    author_email='haimdude@gmail.com',
    url='https://github.com/IdanHaim/RavenDB-Python-Client',
    license='GNU',
    install_requires=
    [
        'pycrypto',
        'requests',
        'inflector',
    ],
	zip_safe=False
)
