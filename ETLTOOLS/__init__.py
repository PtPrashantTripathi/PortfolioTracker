#!/usr/bin/env python

from . import version
from .datetimeutils import DateTimeUtil
from .globalpath import GlobalPath
from .portfolio import Portfolio

__doc__ = f"""PortfolioTracker - ETL TOOL KIT
https://ptprashanttripathi.github.io/PortfolioTracker
Copyright 2023-{DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.version
__author__ = [
    {"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}
]
__all__ = ["DateTimeUtil", "GlobalPath", "utils", "Portfolio"]