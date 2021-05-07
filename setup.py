# MIT License

# Copyright (c) 2021 Travis Dieckmann

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# ==============================================================================

"""
DDB-Compositor
"""
import io
import os
import re

from setuptools import setup, find_packages


def read(*filenames, **kwargs):
    encoding = kwargs.get("encoding", "utf-8")
    sep = kwargs.get("sep", os.linesep)
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


def read_version():
    content = read(os.path.join(os.path.dirname(__file__), "ddb_compositor", "__init__.py"))
    return re.search(r"__version__ = \"([^']+)\"", content).group(1)


def read_requirements(req="base.txt"):
    content = read(os.path.join("requirements", req))
    return [line for line in content.split(os.linesep) if not line.strip().startswith("#")]


setup(
    name="ddb-compositor",
    version=read_version(),
    description="DynamoDB wrapper for a simplified interface to multiple indexes with complex composite keys",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Travis Dieckmann",
    author_email="travis@pseudorandomtech.com",
    url="https://github.com/travisdieckmann/ddb-compositor",
    license="MIT License",
    # Exclude all but the code folders
    packages=find_packages(
        exclude=(
            "tests",
            "tests.*",
            "integration",
            "integration.*",
            "docs",
            "examples",
            "versions",
        )
    ),
    install_requires=read_requirements("base.txt"),
    include_package_data=True,
    extras_require={"dev": read_requirements("dev.txt")},
    keywords="DynamoDB DDB Compositor DDBCompositor DDB-Compositor",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Internet",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Utilities",
    ],
)
