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

import sys

from ddb_compositor.compositor_table import CompositorTable
from ddb_compositor.indexes import (
    PrimaryIndex,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)


if sys.hexversion < 0x30701F0:
    sys.exit("Python 3.7.3 or newer is required by ddb_compositor module.")  # pragma: no cover

__version__ = "0.1.0"
