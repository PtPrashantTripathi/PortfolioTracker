#!/usr/bin/env python

from . import version, portfolio, globalpath, datetimeutils
from .portfolio import *
from .globalpath import *
from .datetimeutils import *

__doc__ = f"""PortfolioTracker v{version.VERSION}- ETL TOOL KIT
https://ptprashanttripathi.github.io/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.VERSION
__author__ = [
    {"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}
]
__all__ = portfolio.__all__ + globalpath.__all__ + datetimeutils.__all__
