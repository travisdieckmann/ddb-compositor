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
import json
from decimal import Decimal
from datetime import date, datetime


class DdbJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if int(o) == o else float(str(o))
        if isinstance(o, datetime):
            return str(o)
        if isinstance(o, date):
            return str(o)
        return super(DdbJsonEncoder, self).default(o)
