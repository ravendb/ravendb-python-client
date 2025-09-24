from setuptools import setup, find_packages

setup(
    name="ravendb",
    packages=find_packages(exclude=["*.tests.*", "tests", "*.tests", "tests.*"]),
    version="7.0.2.post1",
    long_description_content_type="text/markdown",
    long_description=open("README_pypi.md").read(),
    description="Python client for RavenDB NoSQL Database",
    author="RavenDB",
    author_email="support@ravendb.net",
    url="https://github.com/ravendb/ravendb-python-client",
    license="MIT",
    keywords=[
        "ravendb",
        "nosql",
        "database" "pyravendb",
    ],
    python_requires="~=3.9",
    install_requires=[
        "requests~=2.32.0",
        "requests-pkcs12 >= 1.13",
        "pyOpenSSL >= 22.0.0",
        "ijson ~= 3.2.3",
        "websocket-client >= 0.46.0",
        "inflect >= 5.4.0",
    ],
    license_files="LICENSE",
    zip_safe=False,
)
