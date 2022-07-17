from __future__ import annotations

import os
import pathlib
import platform

import versioneer

__author__ = "Ali-Akber Saifee"
__email__ = "ali@indydevs.org"
__copyright__ = "Copyright 2022, Ali-Akber Saifee"

from setuptools import find_packages, setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
PY_IMPLEMENTATION = platform.python_implementation()

def get_requirements(req_file):
    requirements = []

    for r in open(os.path.join(THIS_DIR, "requirements", req_file)).read().splitlines():
        req = r.strip()

        if req.startswith("-r"):
            requirements.extend(get_requirements(req.replace("-r ", "")))
        elif req:
            requirements.append(req)

    return requirements


class coredis_build_ext(build_ext):
    warning_message = """
********************************************************************
{target} could not
be compiled. No C extensions are essential for coredis to run,
although they do result in significant speed improvements for
response parsing.
{comment}
********************************************************************
"""

    def run(self):
        try:
            super().run()
        except Exception as e:
            self.warn(e)
            self.warn(
                self.warning_message.format(
                    target="Extension modules",
                    comment=(
                        "There is an issue with your platform configuration "
                        "- see above."
                    ),
                )
            )

    def build_extension(self, ext):
        try:
            super().build_extension(ext)
        except Exception as e:
            self.warn(e)
            self.warn(
                self.warning_message.format(
                    target=f"The {ext.name} extension ",
                    comment=(
                        "The output above this warning shows how the "
                        "compilation failed."
                    ),
                )
            )


_ROOT_DIR = pathlib.Path(__file__).parent

with open(str(_ROOT_DIR / "README.md")) as f:
    long_description = f.read()

if PY_IMPLEMENTATION == "CPython":
    extensions = [
        Extension(
            name="coredis.speedups",
            sources=["coredis/speedups.c"],
        )
    ]

    try:
        from mypyc.build import mypycify

        extensions += mypycify(
            [
                "coredis/constants.py",
                "coredis/_packer.py",
                "coredis/_unpacker.py",
            ],
            debug_level="0",
            strip_asserts=True,
        )
    except ImportError:
        pass
else:
    extensions = []


setup(
    name="coredis",
    version=versioneer.get_version(),
    description="Python async client for Redis key-value store",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alisaifee/coredis",
    project_urls={
        "Source": "https://github.com/alisaifee/coredis",
        "Documentation": "https://coredis.readthedocs.org",
    },
    author=__author__,
    author_email=__email__,
    maintainer=__author__,
    maintainer_email=__email__,
    keywords=["Redis", "key-value store", "asyncio"],
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data={
        "coredis": ["py.typed"],
    },
    python_requires=">=3.8",
    install_requires=get_requirements("main.txt"),
    cmdclass=versioneer.get_cmdclass(
        {
            "build_ext": coredis_build_ext,
        }
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    ext_modules=extensions,
)
