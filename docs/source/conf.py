#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import types
from typing import List, Tuple

sys.path.insert(0, os.path.abspath("../../"))
sys.path.insert(0, os.path.abspath("./"))

import requests
from theme_config import *

import coredis
import coredis.exceptions

try:
    latest_version = requests.get("https://pypi.org/pypi/coredis/json").json()["info"][
        "version"
    ]
except:
    latest_version = None

master_doc = "index"
project = "coredis"
copyright = "2107, NoneGG | 2023, Ali-Akber Saifee"
author = "alisaifee"
description = "Async redis client for python"

html_static_path = ["./_static"]
html_css_files = [
    "custom.css",
    "https://fonts.googleapis.com/css2?family=Fira+Code:wght@300;400;700&family=Fira+Sans:ital,wght@0,100;0,200;0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,100;1,200;1,300;1,400;1,500;1,600;1,700;1,800;1,900&display=swap",
]

html_baseurl = "https://coredis.readthedocs.io/"
sitemap_url_scheme = "en/stable/{link}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_issues",
    "sphinx_paramlinks",
    "sphinx_sitemap",
    "sphinxcontrib.programoutput",
    "sphinxext.opengraph",
]

autodoc_default_options = {
    "members": True,
    "inherited-members": True,
    "inherit-docstrings": True,
    "member-order": "bysource",
}

ahead = 0

if ".post0.dev" in coredis.__version__:
    version, ahead = coredis.__version__.split(".post0.dev")
else:
    version = coredis.__version__

release = version

html_title = f"{project} <small><b style='color: var(--color-brand-primary)'>{{{release}}}</b></small>"
try:
    ahead = int(ahead)

    if ahead > 0:
        html_theme_options[
            "announcement"
        ] = f"""
        This is a development version. The documentation for the latest version: <b>{latest_version or release}</b> can be found <a href="/en/stable">here</a>
        """
        html_title = f"{project} <small><b style='color: var(--color-brand-primary)'>{{dev}}</b></small>"
except:
    pass

add_module_names = False
autodoc_typehints_format = "short"
autodoc_preserve_defaults = True
autodoc_type_aliases = {
    "KeyT": "~coredis.typing.KeyT",
    "RedisValueT": "~coredis.typing.RedisValueT",
    "StringT": "~coredis.typing.StringT",
    "ResponsePrimitive": "~coredis.typing.ResponsePrimitive",
    "ResponseType": "~coredis.typing.ResponseType",
    "Parameters": "~coredis.typing.Parameters",
    "SubscriptionCallback": "~coredis.commands.pubsub.SubscriptionCallback",
}
autosectionlabel_maxdepth = 3
autosectionlabel_prefix_document = True

extlinks = {
    "pypi": ("https://pypi.org/project/%s", "%s"),
    "redis-version": (
        "https://raw.githubusercontent.com/redis/redis/%s/00-RELEASENOTES",
        "Redis version: %s",
    ),
    "rediscommand": ("https://redis.io/commands/%s", "%s"),
}


issues_github_path = "alisaifee/coredis"


htmlhelp_basename = "coredisdoc"
latex_elements = {}

latex_documents = [
    (master_doc, "coredis.tex", "coredis Documentation", "alisaifee", "manual"),
]
man_pages = [(master_doc, "coredis", "coredis Documentation", [author], 1)]

texinfo_documents = [
    (
        master_doc,
        "coredis",
        "coredis Documentation",
        author,
        "coredis",
        "One line description of project.",
        "Miscellaneous",
    ),
]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "redis-py": ("https://redis-py.readthedocs.io/en/latest/", None),
    "anyio": ("https://anyio.readthedocs.io/en/latest/", None),
    "trio": ("https://trio.readthedocs.io/en/latest/", None),
}

# Workaround for https://github.com/sphinx-doc/sphinx/issues/9560
from sphinx.domains.python import PythonDomain
from sphinx.ext.autodoc import ClassDocumenter, Documenter, _

assert PythonDomain.object_types["data"].roles == ("data", "obj")
PythonDomain.object_types["data"].roles = ("data", "class", "obj")

# Workaround for https://github.com/sphinx-doc/sphinx/issues/10333
from sphinx.util import inspect

inspect.TypeAliasForwardRef.__repr__ = lambda self: self.name
inspect.TypeAliasForwardRef.__hash__ = lambda self: hash(self.name)

original_sort_members = Documenter.sort_members
cmd_group_order = [
    coredis.commands.constants.CommandGroup.STRING,
    coredis.commands.constants.CommandGroup.BITMAP,
    coredis.commands.constants.CommandGroup.GENERIC,
    coredis.commands.constants.CommandGroup.HASH,
    coredis.commands.constants.CommandGroup.HYPERLOGLOG,
    coredis.commands.constants.CommandGroup.LIST,
    coredis.commands.constants.CommandGroup.SET,
    coredis.commands.constants.CommandGroup.SORTED_SET,
    coredis.commands.constants.CommandGroup.GEO,
    coredis.commands.constants.CommandGroup.SCRIPTING,
    coredis.commands.constants.CommandGroup.TRANSACTIONS,
    coredis.commands.constants.CommandGroup.PUBSUB,
    coredis.commands.constants.CommandGroup.STREAM,
    coredis.commands.constants.CommandGroup.SERVER,
    coredis.commands.constants.CommandGroup.CLUSTER,
    coredis.commands.constants.CommandGroup.CONNECTION,
]
preferred_order = {
    "object": 80,
    "method": 90,
    "function": 70,
}


def custom_client_sort(documenter):
    documenter[0].parse_name()
    documenter[0].import_object()
    obj = documenter[0].object
    group_idx = 0
    if hasattr(obj, "__coredis_command"):
        cmd_details = obj.__coredis_command
        group_idx = (
            1 + cmd_group_order.index(cmd_details.group)
            if cmd_details.group in cmd_group_order
            else len(cmd_group_order) + 1
        )
    return (
        -1 * preferred_order.get(type(obj).__name__, documenter[0].member_order),
        group_idx,
        documenter[0].fullname,
    )


def sort_members(
    self, documenters: list[tuple[Documenter, bool]], order: str
) -> list[tuple[Documenter, bool]]:
    if self.name == "coredis.Redis" or self.name == "coredis.RedisCluster":
        documenters.sort(key=custom_client_sort)
        return documenters
    else:
        return original_sort_members(self, documenters, order)


ClassDocumenter.get_overloaded_signatures = lambda *_: []
Documenter.sort_members = sort_members
