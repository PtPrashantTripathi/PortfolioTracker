#!/usr/bin/env python

from . import version
from .datetimeutils import *
from .globalpath import *
from .portfolio import *
from .utils import *

__doc__ = f"""PortfolioTracker - ETL TOOL KIT
https://ptprashanttripathi.github.io/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.version
__author__ = [
    {"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}
]
__all__ = (
    datetimeutils.__all__
    + globalpath.__all__
    + portfolio.__all__
    + utils.__all__
)
