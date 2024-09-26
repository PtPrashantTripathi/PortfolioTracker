#!/usr/bin/env python

from StockETL import version, portfolio, globalpath, datetimeutils
from StockETL.portfolio import *
from StockETL.globalpath import *
from StockETL.datetimeutils import *

__doc__ = f"""StockETL v{version.VERSION}
https://ptprashanttripathi.github.io/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.VERSION
__author__ = [
    {"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}
]
__all__ = portfolio.__all__ + globalpath.__all__ + datetimeutils.__all__
