from __future__ import annotations

import os
import pathlib
import platform
import sys

import versioneer

__author__ = "Ali-Akber Saifee"
__email__ = "ali@indydevs.org"
__copyright__ = "Copyright 2023, Ali-Akber Saifee"

from setuptools import find_packages, setup
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
PY_IMPLEMENTATION = platform.python_implementation()
USE_MYPYC = False
PURE_PYTHON = os.environ.get("PURE_PYTHON", PY_IMPLEMENTATION != "CPython")


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

if len(sys.argv) > 1 and "--use-mypyc" in sys.argv:
    sys.argv.remove("--use-mypyc")
    USE_MYPYC = True

if not PURE_PYTHON:
    extensions = [
        Extension(
            name="coredis.speedups",
            sources=["coredis/speedups.c"],
        )
    ]

    if USE_MYPYC:
        from mypyc.build import mypycify

        extensions += mypycify(
            [
                "coredis/constants.py",
                "coredis/parser.py",
                "coredis/_packer.py",
            ],
            debug_level="0",
            strip_asserts=True,
        )
        for ext in extensions:
            if "-Werror" in ext.extra_compile_args:
                ext.extra_compile_args.remove("-Werror")
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
        "Changes": "https://github.com/alisaifee/coredis/releases",
        "Documentation": "https://coredis.readthedocs.org",
    },
    author=__author__,
    author_email=__email__,
    maintainer=__author__,
    maintainer_email=__email__,
    keywords=["Redis", "key-value store", "asyncio"],
    license="MIT",
    packages=find_packages(exclude=["*tests*"]),
    include_package_data=True,
    package_data={
        "coredis": ["py.typed"],
    },
    python_requires=">=3.9",
    install_requires=get_requirements("main.txt"),
    extras_require={"recipes": get_requirements("recipes.txt")},
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
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    ext_modules=extensions,
)
