#!/usr/bin/env python3
"""
Minimal setup.py for coredis extension modules.
All project metadata is defined in pyproject.toml (PEP-621).
This file only handles C extensions and mypyc compilation.
"""

from __future__ import annotations

import os
from setuptools import Extension
from setuptools.command.build_ext import build_ext


class CoredisBuildExt(build_ext):
    """Custom build_ext that handles mypyc compilation."""

    def run(self):
        """Run the build process with mypyc support."""
        # Check if mypyc should be used
        use_mypyc = os.environ.get("USE_MYPYC", "false").lower() == "true"

        if use_mypyc:
            try:
                from mypyc.build import mypycify

                # Add mypyc extensions
                mypyc_modules = [
                    "coredis/constants.py",
                    "coredis/parser.py",
                    "coredis/_packer.py",
                ]

                mypyc_extensions = mypycify(
                    mypyc_modules,
                    debug_level="0",
                    strip_asserts=True,
                )

                # Remove -Werror from extra_compile_args to avoid build failures

                for ext in mypyc_extensions:
                    if hasattr(ext, "extra_compile_args") and "-Werror" in ext.extra_compile_args:
                        ext.extra_compile_args.remove("-Werror")
                    # Fix the _needs_stub attribute issue

                    if not hasattr(ext, "_needs_stub"):
                        ext._needs_stub = False

                # Add mypyc extensions to the existing extensions
                self.extensions.extend(mypyc_extensions)

            except ImportError:
                print("Warning: mypyc not available, skipping mypyc compilation")
            except Exception as e:
                print(f"Warning: mypyc compilation failed: {e}")

        # Call the parent run method
        super().run()


def get_ext_modules():
    """Get extension modules for the build."""
    extensions = []

    # Add C extension if not pure Python

    if not os.environ.get("PURE_PYTHON", "false").lower() == "true":
        extensions.append(
            Extension(
                name="coredis.speedups",
                sources=["coredis/speedups.c"],
            )
        )

    return extensions


def get_cmdclass():
    """Get custom command classes."""

    return {
        "build_ext": CoredisBuildExt,
    }


# This is the minimal setup.py that only defines extensions
# All other metadata comes from pyproject.toml

if __name__ == "__main__":
    from setuptools import setup

    setup(
        ext_modules=get_ext_modules(),
        cmdclass=get_cmdclass(),
    )
