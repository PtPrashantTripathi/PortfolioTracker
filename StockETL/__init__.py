from StockETL import version, portfolio, globalpath, datetimeutils

__doc__ = f"""StockETL v{version.VERSION}
https://github.com/PtPrashantTripathi/PortfolioTracker
Copyright 2023-{datetimeutils.DateTimeUtil.today().year} Pt. Prashant Tripathi"""
__version__ = version.VERSION
__author__ = [{"name": "ptprashanttripathi", "email": "ptprashanttripathi@outlook.com"}]
__all__ = portfolio.__all__ + globalpath.__all__ + datetimeutils.__all__
__license__ = "MIT"
__maintainer__ = "ptprashanttripathi"
__status__ = "Development"

if __name__ == "__main__":
    print(__doc__)
    print(__version__)
    print(__author__)
    print(__all__)
    print(__license__)
    print(__maintainer__)
    print(__status__)
