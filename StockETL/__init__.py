#!/usr/bin/env python

from . import version, portfolio, globalpath, datetimeutils, reader, writer
from .portfolio import *
from .globalpath import *
from .datetimeutils import *
from .reader import *
from .writer import *

__doc__ = f"""StockETL v{version.VERSION}
https://github.com/PtPrashantTripathi/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.VERSION
__author__ = [{"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}]
__all__ = (
    portfolio.__all__
    + globalpath.__all__
    + datetimeutils.__all__
    + reader.__all__
    + writer.__all__
)
