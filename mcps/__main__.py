"""Enable ``python -m mcps`` invocation.

The console script entry point is ``mcps`` (declared in ``pyproject.toml``
under ``[project.scripts]``), but smoke tests and operators may also want
to invoke the CLI as ``python -m mcps``. This module forwards to
:func:`mcps.cli.main` so both forms are equivalent.
"""

from __future__ import annotations

import sys

from .cli import main


if __name__ == "__main__":
    sys.exit(main())
