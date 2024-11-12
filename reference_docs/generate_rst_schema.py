#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import sys
from os import path
from typing import Any

from sphinx.cmd.quickstart import QuickstartRenderer
from sphinx.ext.apidoc import get_parser, main, recurse_tree, write_file
from sphinx.locale import __
from sphinx.util import ensuredir


def write_master_file(templatedir: str, master_name: str, values: dict, opts: Any):
    template = QuickstartRenderer(templatedir=templatedir)
    opts.destdir = opts.destdir[: opts.destdir.rfind("/")]
    write_file(master_name, template.render(f"{templatedir}/master_doc.rst_t", values), opts)


if __name__ == "__main__":
    parser = get_parser()
    parser.add_argument(
        "--master", metavar="MASTER", default="index", help=__("master document name")
    )
    args = parser.parse_args(sys.argv[1:])

    rootpath = path.abspath(args.module_path)

    # normalize opts
    if args.header is None:
        args.header = rootpath.split(path.sep)[-1]
    args.suffix = args.suffix.removeprefix(".")
    if not path.isdir(rootpath):
        print(__(f"{rootpath} is not a directory."), file=sys.stderr)
        sys.exit(1)
    if not args.dryrun:
        ensuredir(args.destdir)
    excludes = [path.abspath(exclude) for exclude in args.exclude_pattern]
    modules = recurse_tree(rootpath, excludes, args, args.templatedir)

    template_values = {
        "top_modules": [
            {"path": f"api/{module}", "caption": module.split(".")[1].title()}
            for module in modules
            if module.count(".") == 1
        ],
        "maxdepth": args.maxdepth,
    }
    write_master_file(
        templatedir=args.templatedir, master_name=args.master, values=template_values, opts=args
    )
    main()
