#!/usr/bin/env python

from . import datetimeutils, globalpath, portfolio, version
from .datetimeutils import *
from .globalpath import *
from .portfolio import *

__doc__ = f"""PortfolioTracker v{version.VERSION}- ETL TOOL KIT
https://ptprashanttripathi.github.io/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.VERSION
__author__ = [
    {"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}
]
__all__ = portfolio.__all__ + globalpath.__all__ + datetimeutils.__all__
