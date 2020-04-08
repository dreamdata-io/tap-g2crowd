#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-g2crowd",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Dreamdata.io",
    url="https://github.com/dreamdata-io/tap-g2crowd",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_g2crowd"],
    install_requires=[
        "singer-python==5.9.0",
        "requests==2.23.0",
        "ratelimit==2.2.1",
        "backoff==1.8.0",
    ],
    entry_points="""
    [console_scripts]
    tap-g2crowd=tap_g2crowd:main
    """,
    packages=["tap_g2crowd"],
    package_data={"schemas": ["tap_g2crowd/schemas/*.json"]},
    include_package_data=True,
)
