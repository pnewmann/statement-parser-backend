"""
Brokerage Statement Parser API
Extracts positions from Schwab, Fidelity, and other brokerage statements.
Provides portfolio analytics including asset allocation, sector exposure, and risk metrics.
"""

import io
import csv
import re
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import pdfplumber

# Try to import yfinance, pandas, numpy for risk metrics
try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    pd = None
    np = None

app = Flask(__name__)
CORS(app)

# =============================================================================
# ETF/STOCK CLASSIFICATION DATABASE
# Maps symbols to asset class, sector, and geography
# =============================================================================

ETF_CLASSIFICATIONS = {
    # =========================================================================
    # US TREASURY / GOVERNMENT BOND ETFs
    # =========================================================================
    'SGOV': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'BIL': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'SHV': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'SHY': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'IEI': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'IEF': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'TLH': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'TLT': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'EDV': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'GOVT': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'VGSH': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'VGIT': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'VGLT': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'SCHO': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'SCHR': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},
    'SCHQ': {'asset_class': 'Bonds', 'sub_class': 'US Treasury', 'sector': 'Government', 'geography': 'US'},

    # TIPS (Inflation Protected)
    'TIP': {'asset_class': 'Bonds', 'sub_class': 'TIPS', 'sector': 'Government', 'geography': 'US'},
    'VTIP': {'asset_class': 'Bonds', 'sub_class': 'TIPS', 'sector': 'Government', 'geography': 'US'},
    'STIP': {'asset_class': 'Bonds', 'sub_class': 'TIPS', 'sector': 'Government', 'geography': 'US'},
    'SCHP': {'asset_class': 'Bonds', 'sub_class': 'TIPS', 'sector': 'Government', 'geography': 'US'},

    # =========================================================================
    # US AGGREGATE / TOTAL BOND MARKET ETFs
    # =========================================================================
    'AGG': {'asset_class': 'Bonds', 'sub_class': 'US Aggregate', 'sector': 'Broad Market', 'geography': 'US'},
    'BND': {'asset_class': 'Bonds', 'sub_class': 'US Aggregate', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHZ': {'asset_class': 'Bonds', 'sub_class': 'US Aggregate', 'sector': 'Broad Market', 'geography': 'US'},
    'FBND': {'asset_class': 'Bonds', 'sub_class': 'US Aggregate', 'sector': 'Broad Market', 'geography': 'US'},
    'IUSB': {'asset_class': 'Bonds', 'sub_class': 'US Aggregate', 'sector': 'Broad Market', 'geography': 'US'},
    'BSV': {'asset_class': 'Bonds', 'sub_class': 'Short-Term Bond', 'sector': 'Broad Market', 'geography': 'US'},
    'BIV': {'asset_class': 'Bonds', 'sub_class': 'Intermediate Bond', 'sector': 'Broad Market', 'geography': 'US'},
    'BLV': {'asset_class': 'Bonds', 'sub_class': 'Long-Term Bond', 'sector': 'Broad Market', 'geography': 'US'},

    # =========================================================================
    # CORPORATE BOND ETFs
    # =========================================================================
    'LQD': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'VCIT': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'VCSH': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'VCLT': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'IGIB': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'IGSB': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'IGLB': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},
    'SCHI': {'asset_class': 'Bonds', 'sub_class': 'Investment Grade Corp', 'sector': 'Corporate', 'geography': 'US'},

    # High Yield
    'HYG': {'asset_class': 'Bonds', 'sub_class': 'High Yield', 'sector': 'Corporate', 'geography': 'US'},
    'JNK': {'asset_class': 'Bonds', 'sub_class': 'High Yield', 'sector': 'Corporate', 'geography': 'US'},
    'SHYG': {'asset_class': 'Bonds', 'sub_class': 'High Yield', 'sector': 'Corporate', 'geography': 'US'},
    'USHY': {'asset_class': 'Bonds', 'sub_class': 'High Yield', 'sector': 'Corporate', 'geography': 'US'},

    # =========================================================================
    # MUNICIPAL BOND ETFs
    # =========================================================================
    'MUB': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},
    'VTEB': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},
    'TFI': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},
    'SUB': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},
    'SHM': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},
    'SCMB': {'asset_class': 'Bonds', 'sub_class': 'Municipal', 'sector': 'Municipal', 'geography': 'US'},

    # =========================================================================
    # MORTGAGE-BACKED SECURITIES ETFs
    # =========================================================================
    'MBB': {'asset_class': 'Bonds', 'sub_class': 'Mortgage-Backed', 'sector': 'Securitized', 'geography': 'US'},
    'VMBS': {'asset_class': 'Bonds', 'sub_class': 'Mortgage-Backed', 'sector': 'Securitized', 'geography': 'US'},
    'SPMB': {'asset_class': 'Bonds', 'sub_class': 'Mortgage-Backed', 'sector': 'Securitized', 'geography': 'US'},

    # =========================================================================
    # INTERNATIONAL BOND ETFs
    # =========================================================================
    'BNDX': {'asset_class': 'Bonds', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'International'},
    'IAGG': {'asset_class': 'Bonds', 'sub_class': 'International Aggregate', 'sector': 'Broad Market', 'geography': 'International'},
    'BWX': {'asset_class': 'Bonds', 'sub_class': 'International Treasury', 'sector': 'Government', 'geography': 'International'},

    # Emerging Markets Bonds
    'EMB': {'asset_class': 'Bonds', 'sub_class': 'Emerging Markets', 'sector': 'Government', 'geography': 'Emerging Markets'},
    'VWOB': {'asset_class': 'Bonds', 'sub_class': 'Emerging Markets', 'sector': 'Government', 'geography': 'Emerging Markets'},
    'PCY': {'asset_class': 'Bonds', 'sub_class': 'Emerging Markets', 'sector': 'Government', 'geography': 'Emerging Markets'},

    # =========================================================================
    # US TOTAL MARKET STOCK ETFs
    # =========================================================================
    'VTI': {'asset_class': 'Stocks', 'sub_class': 'US Total Market', 'sector': 'Broad Market', 'geography': 'US'},
    'ITOT': {'asset_class': 'Stocks', 'sub_class': 'US Total Market', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHB': {'asset_class': 'Stocks', 'sub_class': 'US Total Market', 'sector': 'Broad Market', 'geography': 'US'},
    'SPTM': {'asset_class': 'Stocks', 'sub_class': 'US Total Market', 'sector': 'Broad Market', 'geography': 'US'},
    'IWV': {'asset_class': 'Stocks', 'sub_class': 'US Total Market', 'sector': 'Broad Market', 'geography': 'US'},

    # =========================================================================
    # US LARGE CAP ETFs
    # =========================================================================
    'SPY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'VOO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IVV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SPLG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'VV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IWB': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'MGC': {'asset_class': 'Stocks', 'sub_class': 'US Mega Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'OEF': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Broad Market', 'geography': 'US'},

    # Large Cap Growth
    'QQQ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Technology', 'geography': 'US'},
    'QQQM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Technology', 'geography': 'US'},
    'VUG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'IWF': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'SPYG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'VOOG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'MGK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'IVW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'IUSG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},

    # Large Cap Value
    'VTV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'IWD': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'SPYV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'VOOV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'MGV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'IVE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'IUSV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'RPV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Value', 'sector': 'Broad Market', 'geography': 'US'},

    # =========================================================================
    # US MID CAP ETFs
    # =========================================================================
    'VO': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IJH': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHM': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IWR': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SPMD': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'MDY': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'VOT': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'VOE': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'IWP': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'IWS': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap Value', 'sector': 'Broad Market', 'geography': 'US'},

    # =========================================================================
    # US SMALL CAP ETFs
    # =========================================================================
    'VB': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IJR': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'IWM': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHA': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SPSM': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'SLY': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},
    'VBK': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'VBR': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'IWO': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'IWN': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap Value', 'sector': 'Broad Market', 'geography': 'US'},
    'VIOO': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Broad Market', 'geography': 'US'},

    # =========================================================================
    # US DIVIDEND ETFs
    # =========================================================================
    'VIG': {'asset_class': 'Stocks', 'sub_class': 'US Dividend Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'VYM': {'asset_class': 'Stocks', 'sub_class': 'US High Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHD': {'asset_class': 'Stocks', 'sub_class': 'US Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'DVY': {'asset_class': 'Stocks', 'sub_class': 'US High Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'SDY': {'asset_class': 'Stocks', 'sub_class': 'US Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'DGRO': {'asset_class': 'Stocks', 'sub_class': 'US Dividend Growth', 'sector': 'Broad Market', 'geography': 'US'},
    'NOBL': {'asset_class': 'Stocks', 'sub_class': 'US Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'SPYD': {'asset_class': 'Stocks', 'sub_class': 'US High Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'HDV': {'asset_class': 'Stocks', 'sub_class': 'US High Dividend', 'sector': 'Broad Market', 'geography': 'US'},
    'SCHY': {'asset_class': 'Stocks', 'sub_class': 'International Dividend', 'sector': 'Broad Market', 'geography': 'International'},

    # =========================================================================
    # INTERNATIONAL DEVELOPED MARKET ETFs
    # =========================================================================
    'VEA': {'asset_class': 'Stocks', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'Developed Markets'},
    'IEFA': {'asset_class': 'Stocks', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'Developed Markets'},
    'EFA': {'asset_class': 'Stocks', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'Developed Markets'},
    'SCHF': {'asset_class': 'Stocks', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'Developed Markets'},
    'SPDW': {'asset_class': 'Stocks', 'sub_class': 'International Developed', 'sector': 'Broad Market', 'geography': 'Developed Markets'},
    'VGK': {'asset_class': 'Stocks', 'sub_class': 'Europe', 'sector': 'Broad Market', 'geography': 'Europe'},
    'VPL': {'asset_class': 'Stocks', 'sub_class': 'Pacific', 'sector': 'Broad Market', 'geography': 'Asia Pacific'},
    'EWJ': {'asset_class': 'Stocks', 'sub_class': 'Japan', 'sector': 'Broad Market', 'geography': 'Japan'},
    'EWG': {'asset_class': 'Stocks', 'sub_class': 'Germany', 'sector': 'Broad Market', 'geography': 'Europe'},
    'EWU': {'asset_class': 'Stocks', 'sub_class': 'UK', 'sector': 'Broad Market', 'geography': 'Europe'},
    'EWC': {'asset_class': 'Stocks', 'sub_class': 'Canada', 'sector': 'Broad Market', 'geography': 'North America'},
    'EWA': {'asset_class': 'Stocks', 'sub_class': 'Australia', 'sector': 'Broad Market', 'geography': 'Asia Pacific'},
    'IEUR': {'asset_class': 'Stocks', 'sub_class': 'Europe', 'sector': 'Broad Market', 'geography': 'Europe'},
    'IPAC': {'asset_class': 'Stocks', 'sub_class': 'Pacific', 'sector': 'Broad Market', 'geography': 'Asia Pacific'},

    # =========================================================================
    # EMERGING MARKETS ETFs
    # =========================================================================
    'VWO': {'asset_class': 'Stocks', 'sub_class': 'Emerging Markets', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'IEMG': {'asset_class': 'Stocks', 'sub_class': 'Emerging Markets', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'EEM': {'asset_class': 'Stocks', 'sub_class': 'Emerging Markets', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'SCHE': {'asset_class': 'Stocks', 'sub_class': 'Emerging Markets', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'SPEM': {'asset_class': 'Stocks', 'sub_class': 'Emerging Markets', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'MCHI': {'asset_class': 'Stocks', 'sub_class': 'China', 'sector': 'Broad Market', 'geography': 'China'},
    'FXI': {'asset_class': 'Stocks', 'sub_class': 'China', 'sector': 'Broad Market', 'geography': 'China'},
    'KWEB': {'asset_class': 'Stocks', 'sub_class': 'China', 'sector': 'Technology', 'geography': 'China'},
    'EWZ': {'asset_class': 'Stocks', 'sub_class': 'Brazil', 'sector': 'Broad Market', 'geography': 'Latin America'},
    'EWT': {'asset_class': 'Stocks', 'sub_class': 'Taiwan', 'sector': 'Broad Market', 'geography': 'Asia Pacific'},
    'EWY': {'asset_class': 'Stocks', 'sub_class': 'South Korea', 'sector': 'Broad Market', 'geography': 'Asia Pacific'},
    'INDA': {'asset_class': 'Stocks', 'sub_class': 'India', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},
    'EPI': {'asset_class': 'Stocks', 'sub_class': 'India', 'sector': 'Broad Market', 'geography': 'Emerging Markets'},

    # =========================================================================
    # TOTAL WORLD / GLOBAL ETFs
    # =========================================================================
    'VT': {'asset_class': 'Stocks', 'sub_class': 'Global', 'sector': 'Broad Market', 'geography': 'Global'},
    'ACWI': {'asset_class': 'Stocks', 'sub_class': 'Global', 'sector': 'Broad Market', 'geography': 'Global'},
    'URTH': {'asset_class': 'Stocks', 'sub_class': 'Global', 'sector': 'Broad Market', 'geography': 'Global'},
    'VXUS': {'asset_class': 'Stocks', 'sub_class': 'International Total', 'sector': 'Broad Market', 'geography': 'International'},
    'IXUS': {'asset_class': 'Stocks', 'sub_class': 'International Total', 'sector': 'Broad Market', 'geography': 'International'},
    'VEU': {'asset_class': 'Stocks', 'sub_class': 'International Total', 'sector': 'Broad Market', 'geography': 'International'},
    'VSS': {'asset_class': 'Stocks', 'sub_class': 'International Small Cap', 'sector': 'Broad Market', 'geography': 'International'},
    'ACWX': {'asset_class': 'Stocks', 'sub_class': 'International Total', 'sector': 'Broad Market', 'geography': 'International'},

    # =========================================================================
    # US SECTOR ETFs - TECHNOLOGY
    # =========================================================================
    'XLK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'VGT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'IYW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'FTEC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'IGV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'SOXX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'SMH': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'ARKK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Technology', 'geography': 'US'},
    'ARKW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Technology', 'geography': 'US'},
    'ROBO': {'asset_class': 'Stocks', 'sub_class': 'Global', 'sector': 'Technology', 'geography': 'Global'},
    'BOTZ': {'asset_class': 'Stocks', 'sub_class': 'Global', 'sector': 'Technology', 'geography': 'Global'},

    # =========================================================================
    # US SECTOR ETFs - HEALTHCARE
    # =========================================================================
    'XLV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'VHT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'IYH': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'FHLC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'IBB': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'XBI': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'IHI': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'ARKG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap Growth', 'sector': 'Healthcare', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - FINANCIALS
    # =========================================================================
    'XLF': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'VFH': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'IYF': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'FNCL': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'KRE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'KBE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'IAI': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - ENERGY
    # =========================================================================
    'XLE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'VDE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'IYE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'FENY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'OIH': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'XOP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'AMLP': {'asset_class': 'Stocks', 'sub_class': 'MLPs', 'sector': 'Energy', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - CONSUMER
    # =========================================================================
    'XLY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'VCR': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'IYC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'FDIS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'XLP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'VDC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'IYK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'FSTA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - INDUSTRIALS
    # =========================================================================
    'XLI': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'VIS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'IYJ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'FIDU': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'ITA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'XAR': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - UTILITIES
    # =========================================================================
    'XLU': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
    'VPU': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
    'IDU': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
    'FUTY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - MATERIALS
    # =========================================================================
    'XLB': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Materials', 'geography': 'US'},
    'VAW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Materials', 'geography': 'US'},
    'IYM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Materials', 'geography': 'US'},
    'FMAT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Materials', 'geography': 'US'},

    # =========================================================================
    # US SECTOR ETFs - COMMUNICATION SERVICES
    # =========================================================================
    'XLC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'VOX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'IYZ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'FCOM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},

    # =========================================================================
    # REAL ESTATE ETFs
    # =========================================================================
    'VNQ': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'XLRE': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'IYR': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'SCHH': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'FREL': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'RWR': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'USRT': {'asset_class': 'Real Estate', 'sub_class': 'US REITs', 'sector': 'Real Estate', 'geography': 'US'},
    'VNQI': {'asset_class': 'Real Estate', 'sub_class': 'International REITs', 'sector': 'Real Estate', 'geography': 'International'},
    'RWX': {'asset_class': 'Real Estate', 'sub_class': 'International REITs', 'sector': 'Real Estate', 'geography': 'International'},
    'IFGL': {'asset_class': 'Real Estate', 'sub_class': 'International REITs', 'sector': 'Real Estate', 'geography': 'International'},

    # =========================================================================
    # CRYPTOCURRENCY ETFs
    # =========================================================================
    'IBIT': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'FBTC': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'GBTC': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'ARKB': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'BITB': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'BTCO': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'BTCW': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'HODL': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'BRRR': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'EZBC': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'DEFI': {'asset_class': 'Crypto', 'sub_class': 'DeFi', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'ETHA': {'asset_class': 'Crypto', 'sub_class': 'Ethereum', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'ETHE': {'asset_class': 'Crypto', 'sub_class': 'Ethereum', 'sector': 'Cryptocurrency', 'geography': 'Global'},

    # =========================================================================
    # COMMODITY ETFs
    # =========================================================================
    'GLD': {'asset_class': 'Commodities', 'sub_class': 'Gold', 'sector': 'Precious Metals', 'geography': 'Global'},
    'IAU': {'asset_class': 'Commodities', 'sub_class': 'Gold', 'sector': 'Precious Metals', 'geography': 'Global'},
    'GLDM': {'asset_class': 'Commodities', 'sub_class': 'Gold', 'sector': 'Precious Metals', 'geography': 'Global'},
    'SGOL': {'asset_class': 'Commodities', 'sub_class': 'Gold', 'sector': 'Precious Metals', 'geography': 'Global'},
    'SLV': {'asset_class': 'Commodities', 'sub_class': 'Silver', 'sector': 'Precious Metals', 'geography': 'Global'},
    'PPLT': {'asset_class': 'Commodities', 'sub_class': 'Platinum', 'sector': 'Precious Metals', 'geography': 'Global'},
    'PALL': {'asset_class': 'Commodities', 'sub_class': 'Palladium', 'sector': 'Precious Metals', 'geography': 'Global'},
    'DBC': {'asset_class': 'Commodities', 'sub_class': 'Broad Commodities', 'sector': 'Commodities', 'geography': 'Global'},
    'GSG': {'asset_class': 'Commodities', 'sub_class': 'Broad Commodities', 'sector': 'Commodities', 'geography': 'Global'},
    'PDBC': {'asset_class': 'Commodities', 'sub_class': 'Broad Commodities', 'sector': 'Commodities', 'geography': 'Global'},
    'USO': {'asset_class': 'Commodities', 'sub_class': 'Oil', 'sector': 'Energy', 'geography': 'Global'},
    'UNG': {'asset_class': 'Commodities', 'sub_class': 'Natural Gas', 'sector': 'Energy', 'geography': 'Global'},
    'DBA': {'asset_class': 'Commodities', 'sub_class': 'Agriculture', 'sector': 'Agriculture', 'geography': 'Global'},
    'CORN': {'asset_class': 'Commodities', 'sub_class': 'Agriculture', 'sector': 'Agriculture', 'geography': 'Global'},
    'WEAT': {'asset_class': 'Commodities', 'sub_class': 'Agriculture', 'sector': 'Agriculture', 'geography': 'Global'},

    # =========================================================================
    # CASH / MONEY MARKET
    # =========================================================================
    'CASH': {'asset_class': 'Cash', 'sub_class': 'Cash', 'sector': 'Money Market', 'geography': 'US'},
    'SPAXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'FDRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'VMFXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'SWVXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},

    # =========================================================================
    # MAJOR INDIVIDUAL STOCKS
    # =========================================================================
    # Technology
    'AAPL': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'MSFT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'GOOGL': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'GOOG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'AMZN': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'NVDA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'META': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'TSLA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'AVGO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'ADBE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'CRM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'ORCL': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'CSCO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'ACN': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'IBM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'INTC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'AMD': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'QCOM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'TXN': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Technology', 'geography': 'US'},
    'NFLX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'PYPL': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},

    # Financials
    'BRK.B': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'BRK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'JPM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'V': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'MA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'BAC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'WFC': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'GS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'MS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'C': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'AXP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},

    # Healthcare
    'UNH': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'JNJ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'LLY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'PFE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'ABBV': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'MRK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'TMO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'ABT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'DHR': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},
    'BMY': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Healthcare', 'geography': 'US'},

    # Consumer
    'WMT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'PG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'KO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'PEP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'COST': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Staples', 'geography': 'US'},
    'HD': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'MCD': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'NKE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'SBUX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'TGT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'LOW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Consumer Discretionary', 'geography': 'US'},
    'DIS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},

    # Energy
    'XOM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'CVX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'COP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'SLB': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'EOG': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},

    # Industrials
    'GE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'CAT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'BA': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'HON': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'UPS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'RTX': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'LMT': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'MMM': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},
    'DE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Industrials', 'geography': 'US'},

    # Telecom / Utilities
    'VZ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'T': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'TMUS': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Communication Services', 'geography': 'US'},
    'NEE': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
    'DUK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
    'SO': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Utilities', 'geography': 'US'},
}

# S&P 500 sector weights for benchmark comparison (approximate)
SP500_SECTOR_WEIGHTS = {
    'Technology': 0.29,
    'Healthcare': 0.13,
    'Financials': 0.13,
    'Consumer Discretionary': 0.10,
    'Communication Services': 0.09,
    'Industrials': 0.08,
    'Consumer Staples': 0.06,
    'Energy': 0.04,
    'Utilities': 0.03,
    'Real Estate': 0.02,
    'Materials': 0.03,
}

# Common stock/ETF symbols pattern
SYMBOL_PATTERN = re.compile(r'^[A-Z]{1,5}$')

# Words that look like symbols but aren't
EXCLUDED_WORDS = {
    'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HAD', 'HER',
    'WAS', 'ONE', 'OUR', 'OUT', 'HAS', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW',
    'OLD', 'SEE', 'WAY', 'WHO', 'BOY', 'DID', 'GET', 'LET', 'PUT', 'SAY', 'SHE',
    'TOO', 'USE', 'DAY', 'ANY', 'YTD', 'ETF', 'IRA', 'USA', 'USD', 'TAX', 'FEE',
    'TOTAL', 'CASH', 'BANK', 'DATE', 'TYPE', 'FUND', 'BOND', 'NOTE', 'COST',
    'GAIN', 'LOSS', 'RATE', 'YEAR', 'TERM', 'PAGE', 'CUSIP', 'PRICE', 'VALUE',
    'SHARE', 'ACCT', 'VISIT', 'TERMS', 'FUNDS', 'SWEEP', 'INCOME', 'PERIOD',
    'SYMBOL', 'ACTION', 'MARGIN', 'ACCOUNT', 'SUMMARY', 'BALANCE', 'INTEREST',
    'DIVIDEND', 'PURCHASE', 'CATEGORY', 'QUANTITY', 'DESCRIPTION', 'POSITIONS',
    'IN', 'OF', 'TO', 'OR', 'IF', 'AT', 'BY', 'ON', 'AS', 'IS', 'IT', 'BE', 'WE',
    'AN', 'DO', 'SO', 'UP', 'NO', 'GO', 'MY', 'US', 'AM', 'HE', 'ME',
    'A', 'I', 'X', 'Z',
    'HELD', 'THAT', 'THIS', 'WITH', 'FROM', 'HAVE', 'BEEN', 'EACH', 'WILL',
    'MORE', 'WHEN', 'THEM', 'BEEN', 'CALL', 'FIRST', 'WATER', 'THAN', 'LONG',
    'EL', 'TX', 'CA', 'NY', 'FL', 'CO', 'AZ', 'NC', 'VA', 'WA', 'MA', 'PA',
}

# Known ETF/Stock symbols - comprehensive list
KNOWN_SYMBOLS = {
    # Bond ETFs
    'SGOV', 'AGG', 'BND', 'BNDX', 'VTIP', 'STIP', 'TIP', 'TIPS', 'SCHZ', 'SCHP',
    'EMB', 'VWOB', 'LQD', 'HYG', 'JNK', 'MUB', 'TLT', 'IEF', 'SHY', 'GOVT',
    'VCIT', 'VCSH', 'BSV', 'BIV', 'BLV', 'VMBS', 'MBB', 'IGIB', 'IGSB',
    # US Equity ETFs
    'VTI', 'VOO', 'SPY', 'QQQ', 'IVV', 'IWM', 'IWF', 'IWD', 'VIG', 'VYM',
    'SCHD', 'SCHA', 'SCHB', 'SCHF', 'SCHE', 'SCHX', 'SCHY', 'SCHG', 'SCHV',
    'SPMD', 'SPSM', 'SPYM', 'SPLG', 'SPTM', 'SPYG', 'SPYV',
    'VB', 'VV', 'VO', 'VBR', 'VBK', 'VOE', 'VOT', 'VTV', 'VUG', 'MGK', 'MGV',
    'ITOT', 'IXUS', 'IJH', 'IJR', 'IWB', 'IWR', 'IWS', 'IWN', 'IWO', 'IWP',
    # International ETFs
    'VEA', 'VWO', 'IEFA', 'IEMG', 'EFA', 'EEM', 'VXUS', 'VEU', 'VSS', 'VGK',
    'IXUS', 'ACWI', 'ACWX', 'VPL', 'EWJ', 'EWZ', 'EWY', 'EWT', 'MCHI', 'FXI',
    # Crypto ETFs
    'IBIT', 'ETHA', 'GBTC', 'FBTC', 'ARKB', 'BITB', 'HODL', 'BRRR', 'EZBC',
    'BTCO', 'BTCW', 'DEFI', 'ETHE',
    # Sector ETFs
    'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE',
    'VGT', 'VFH', 'VDE', 'VHT', 'VIS', 'VCR', 'VDC', 'VPU', 'VAW', 'VNQ',
    # Individual stocks
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK',
    'JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'VZ',
    'ADBE', 'NFLX', 'CRM', 'PFE', 'TMO', 'PEP', 'AVGO', 'CSCO', 'ACN',
    'WMT', 'KO', 'MRK', 'ABT', 'CVX', 'XOM', 'LLY', 'ABBV', 'ORCL', 'AMD',
    'INTC', 'QCOM', 'TXN', 'IBM', 'GE', 'CAT', 'BA', 'MMM', 'HON', 'UPS',
}


# Common words found in fund descriptions for splitting
DESCRIPTION_WORDS = [
    'ISHARES', 'VANGUARD', 'SCHWAB', 'FIDELITY', 'STATE', 'STREET', 'SPDR',
    'BITCOIN', 'ETHEREUM', 'CRYPTO', 'DIGITAL', 'TRUST', 'ETF', 'FUND',
    'TOTAL', 'STOCK', 'BOND', 'MARKET', 'INDEX', 'CORE', 'AGGREGATE',
    'TREASURY', 'GOVERNMENT', 'CORPORATE', 'MUNICIPAL', 'HIGH', 'YIELD',
    'SHORT', 'TERM', 'LONG', 'INTERMEDIATE', 'ULTRA', 'EXTENDED',
    'SMALL', 'MID', 'LARGE', 'CAP', 'VALUE', 'GROWTH', 'BLEND',
    'INTERNATIONAL', 'GLOBAL', 'WORLD', 'EMERGING', 'DEVELOPED', 'MARKETS',
    'EUROPE', 'PACIFIC', 'ASIA', 'JAPAN', 'CHINA', 'INDIA', 'BRAZIL',
    'FTSE', 'MSCI', 'RUSSELL', 'S&P', 'DOW', 'NASDAQ', 'NYSE',
    'DIVIDEND', 'INCOME', 'APPRECIATION', 'EQUITY', 'SECURITIES',
    'INFLATION', 'PROTECTED', 'TIPS', 'REAL', 'ESTATE', 'REIT',
    'TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL', 'ENERGY', 'UTILITIES',
    'CONSUMER', 'INDUSTRIAL', 'MATERIALS', 'COMMUNICATION', 'SERVICES',
    'PORTFOLIO', 'BALANCED', 'CONSERVATIVE', 'MODERATE', 'AGGRESSIVE',
    'US', 'USA', 'USD', 'JP', 'MORGAN', 'JPMORGAN', 'BLACKROCK',
    'MONTH', 'YEAR', 'DURATION', 'MATURITY', 'FLOATING', 'RATE',
    'INVESTMENT', 'GRADE', 'JUNK', 'CONVERTIBLE', 'PREFERRED',
    'EX', 'ALL', 'WORLD', 'ACWI', 'EAFE', 'EMU',
    'PRTFL', 'PORTFL', 'MTS', 'MARKT', 'INFPROT', 'SHRT', 'INF', 'PROT',
]


def split_description(text):
    """Split concatenated description into readable words."""
    if not text:
        return ''

    # Already has spaces
    if ' ' in text:
        return text

    text = text.upper()
    result = []
    remaining = text

    while remaining:
        matched = False
        # Try to match longest words first
        for word in sorted(DESCRIPTION_WORDS, key=len, reverse=True):
            if remaining.startswith(word):
                result.append(word)
                remaining = remaining[len(word):]
                matched = True
                break

        if not matched:
            # No known word matched, take one character and continue
            # But try to find the next known word boundary
            found_next = False
            for i in range(1, len(remaining)):
                for word in DESCRIPTION_WORDS:
                    if remaining[i:].startswith(word):
                        result.append(remaining[:i])
                        remaining = remaining[i:]
                        found_next = True
                        break
                if found_next:
                    break

            if not found_next:
                # No more known words, append rest
                result.append(remaining)
                break

    # Join and clean up
    description = ' '.join(result)
    # Fix common patterns
    description = description.replace('S & P', 'S&P')
    description = description.replace('JP MORGAN', 'JPMORGAN')
    return description.strip()


def clean_number(value):
    """Convert string number to float, handling commas and dollar signs."""
    if not value:
        return None
    cleaned = re.sub(r'[$,]', '', str(value).strip())
    # Remove parentheses for negative numbers
    if cleaned.startswith('(') and cleaned.endswith(')'):
        cleaned = '-' + cleaned[1:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def is_valid_symbol(text):
    """Check if text looks like a stock symbol."""
    if not text:
        return False
    text = text.strip().upper()

    if text in EXCLUDED_WORDS:
        return False

    if text in KNOWN_SYMBOLS:
        return True

    if not SYMBOL_PATTERN.match(text):
        if not re.match(r'^[A-Z]{1,4}\.[A-Z]$', text):
            return False

    if len(text) <= 2 and text not in KNOWN_SYMBOLS:
        return False

    return True


def detect_brokerage_pdf(text):
    """Detect which brokerage the PDF is from."""
    text_lower = text.lower()
    if 'charles schwab' in text_lower or 'schwab' in text_lower:
        return 'schwab'
    elif 'fidelity' in text_lower:
        return 'fidelity'
    elif 'vanguard' in text_lower:
        return 'vanguard'
    elif 'td ameritrade' in text_lower:
        return 'tdameritrade'
    elif 'e*trade' in text_lower or 'etrade' in text_lower:
        return 'etrade'
    elif 'robinhood' in text_lower:
        return 'robinhood'
    return 'unknown'


def parse_schwab_pdf(pdf):
    """Parse Charles Schwab brokerage statement using text extraction."""
    positions = []

    # Get all text from the PDF
    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    # Look for ETF positions section
    # Pattern: SYMBOL DESCRIPTION QUANTITY PRICE MARKETVALUE ...
    # Example: AGG ISHARESCOREUS ... 273.9131 99.88000 27,358.44

    # Split into lines
    lines = full_text.split('\n')

    in_etf_section = False
    in_cash_section = False

    for line in lines:
        # Check for section headers
        if 'Exchange Traded Funds' in line:
            in_etf_section = True
            in_cash_section = False
            continue
        if 'Cash and Cash Investments' in line:
            in_cash_section = True
            in_etf_section = False
            continue
        if 'Transaction' in line or 'Positions - Summary' in line:
            in_etf_section = False
            in_cash_section = False
            continue

        # Parse ETF positions
        if in_etf_section:
            # Skip header lines
            if 'Symbol' in line and 'Description' in line:
                continue
            if 'Total' in line and ('Exchange' in line or 'Traded' in line):
                continue

            # Try to match position line pattern
            # Format: SYMBOL DESCRIPTION, QUANTITY PRICE MARKETVALUE ...
            # Example: AGG ISHARESCOREUS, 111.0000 99.88000 11,086.68

            # First, try to match a line starting with a known symbol
            matched = False
            for symbol in KNOWN_SYMBOLS:
                if line.startswith(symbol + ' '):
                    matched = True
                    # Extract all numbers from the line
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)

                    if len(numbers) >= 3:
                        quantity = clean_number(numbers[0])
                        price = clean_number(numbers[1])
                        market_value = clean_number(numbers[2])

                        # Extract description: everything between symbol and first number
                        # Find where the first number starts
                        first_num_match = re.search(r'[\d,]+\.[\d]+', line)
                        if first_num_match:
                            desc_end = first_num_match.start()
                            description = line[len(symbol):desc_end].strip()
                            # Clean up: remove special chars, trailing commas
                            description = re.sub(r'[,◊\(\)]', '', description).strip()
                            # Split concatenated words
                            description = split_description(description)
                        else:
                            description = ''

                        position = {
                            'symbol': symbol,
                            'description': description,
                            'shares': round(quantity, 4) if quantity else None,
                            'price': round(price, 2) if price else None,
                            'value': round(market_value, 2) if market_value else None
                        }

                        if not any(p['symbol'] == symbol for p in positions):
                            positions.append(position)
                    break

            # If no known symbol matched, try generic pattern
            if not matched:
                # Match: 3-5 letter symbol at start, followed by description and numbers
                match = re.match(r'^([A-Z]{2,5})\s+([A-Za-z0-9\-]+)', line)
                if match:
                    symbol = match.group(1)
                    if is_valid_symbol(symbol):
                        numbers = re.findall(r'[\d,]+\.[\d]+', line)
                        if len(numbers) >= 3:
                            quantity = clean_number(numbers[0])
                            price = clean_number(numbers[1])
                            market_value = clean_number(numbers[2])

                            # Extract description
                            first_num_match = re.search(r'[\d,]+\.[\d]+', line)
                            if first_num_match:
                                desc_end = first_num_match.start()
                                description = line[len(symbol):desc_end].strip()
                                description = re.sub(r'[,◊\(\)]', '', description).strip()
                                description = split_description(description)
                            else:
                                description = ''

                            position = {
                                'symbol': symbol,
                                'description': description,
                                'shares': round(quantity, 4) if quantity else None,
                                'price': round(price, 2) if price else None,
                                'value': round(market_value, 2) if market_value else None
                            }

                            if not any(p['symbol'] == symbol for p in positions):
                                positions.append(position)

        # Parse cash positions
        if in_cash_section:
            if 'CHARLESSCHWAB' in line.replace(' ', '') or 'SCHWABBANK' in line.replace(' ', ''):
                if 'Total' in line:
                    continue
                # Extract the ending balance
                numbers = re.findall(r'[\d,]+\.[\d]{2}', line)
                if len(numbers) >= 2:
                    # Second number is usually ending balance
                    ending_balance = clean_number(numbers[1])
                    if ending_balance and ending_balance > 0:
                        if not any(p['symbol'] == 'CASH' for p in positions):
                            positions.append({
                                'symbol': 'CASH',
                                'description': 'Charles Schwab Bank Sweep',
                                'shares': None,
                                'price': None,
                                'value': round(ending_balance, 2)
                            })

    return positions


def parse_fidelity_pdf(pdf):
    """Parse Fidelity brokerage statement."""
    positions = []

    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    lines = full_text.split('\n')

    for line in lines:
        # Look for known symbols
        for symbol in KNOWN_SYMBOLS:
            if symbol in line:
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                if len(numbers) >= 2:
                    position = {
                        'symbol': symbol,
                        'description': '',
                        'shares': clean_number(numbers[0]) if numbers else None,
                        'price': None,
                        'value': clean_number(numbers[-1]) if numbers else None
                    }

                    if position['shares'] and position['value']:
                        position['price'] = round(position['value'] / position['shares'], 2)

                    if not any(p['symbol'] == symbol for p in positions):
                        positions.append(position)
                break

    return positions


def parse_csv_file(content):
    """Parse a CSV file from various brokerages."""
    positions = []

    if isinstance(content, bytes):
        content = content.decode('utf-8-sig')

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)

    if not rows:
        return positions

    # Find header row
    header_row = None
    header_index = 0

    for i, row in enumerate(rows):
        row_lower = [str(cell).lower() for cell in row]
        if any(h in row_lower for h in ['symbol', 'ticker', 'security']):
            header_row = row
            header_index = i
            break

    if not header_row:
        header_row = rows[0]
        header_index = 0

    header_lower = [str(h).lower().strip() for h in header_row]

    symbol_idx = None
    desc_idx = None
    shares_idx = None
    price_idx = None
    value_idx = None

    for i, h in enumerate(header_lower):
        if any(x in h for x in ['symbol', 'ticker']):
            symbol_idx = i
        elif any(x in h for x in ['description', 'security', 'name']):
            desc_idx = i
        elif any(x in h for x in ['quantity', 'shares', 'units']):
            shares_idx = i
        elif any(x in h for x in ['price', 'last']):
            price_idx = i
        elif any(x in h for x in ['value', 'market value', 'amount', 'balance']):
            value_idx = i

    for row in rows[header_index + 1:]:
        if len(row) <= max(filter(None, [symbol_idx, desc_idx, shares_idx, price_idx, value_idx]), default=0):
            continue

        symbol = None
        if symbol_idx is not None and symbol_idx < len(row):
            symbol = str(row[symbol_idx]).strip().upper()

        if not symbol or not is_valid_symbol(symbol):
            for cell in row:
                if is_valid_symbol(str(cell).strip()):
                    symbol = str(cell).strip().upper()
                    break

        if not symbol or not is_valid_symbol(symbol):
            continue

        position = {
            'symbol': symbol,
            'description': str(row[desc_idx]).strip() if desc_idx is not None and desc_idx < len(row) else '',
            'shares': clean_number(row[shares_idx]) if shares_idx is not None and shares_idx < len(row) else None,
            'price': clean_number(row[price_idx]) if price_idx is not None and price_idx < len(row) else None,
            'value': clean_number(row[value_idx]) if value_idx is not None and value_idx < len(row) else None,
        }

        if position['shares'] or position['value']:
            positions.append(position)

    return positions


def parse_pdf_file(content):
    """Parse a PDF brokerage statement."""
    positions = []

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        full_text = ""
        for page in pdf.pages[:3]:
            full_text += (page.extract_text() or "") + "\n"

        brokerage = detect_brokerage_pdf(full_text)

        if brokerage == 'schwab':
            positions = parse_schwab_pdf(pdf)
        elif brokerage == 'fidelity':
            positions = parse_fidelity_pdf(pdf)
        else:
            # Generic text-based parsing
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines = text.split('\n')

                for line in lines:
                    for symbol in KNOWN_SYMBOLS:
                        if symbol in line:
                            numbers = re.findall(r'[\d,]+\.[\d]+', line)
                            if len(numbers) >= 2:
                                position = {
                                    'symbol': symbol,
                                    'description': '',
                                    'shares': clean_number(numbers[0]),
                                    'price': None,
                                    'value': clean_number(numbers[-1])
                                }
                                if not any(p['symbol'] == symbol for p in positions):
                                    positions.append(position)
                            break

    # Remove duplicates
    seen = set()
    unique_positions = []
    for p in positions:
        if p['symbol'] not in seen:
            seen.add(p['symbol'])
            unique_positions.append(p)

    return unique_positions, brokerage


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'})


@app.route('/parse', methods=['POST'])
def parse_statement():
    """Parse an uploaded brokerage statement."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']

    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    filename = file.filename.lower()
    content = file.read()

    try:
        if filename.endswith('.csv'):
            positions = parse_csv_file(content)
            brokerage = 'csv'
        elif filename.endswith('.pdf'):
            positions, brokerage = parse_pdf_file(content)
        else:
            return jsonify({'error': 'Unsupported file type. Please upload a PDF or CSV file.'}), 400

        if not positions:
            return jsonify({
                'error': 'No positions found. The file format may not be supported or the statement may be empty.',
                'positions': [],
                'brokerage': brokerage
            }), 200

        return jsonify({
            'positions': positions,
            'count': len(positions),
            'brokerage': brokerage
        })

    except Exception as e:
        return jsonify({'error': f'Error parsing file: {str(e)}'}), 500


# =============================================================================
# PORTFOLIO ANALYTICS
# =============================================================================

def get_classification(symbol):
    """Get classification for a symbol, with fallback for unknown symbols."""
    symbol = symbol.upper().strip()

    if symbol in ETF_CLASSIFICATIONS:
        return ETF_CLASSIFICATIONS[symbol]

    # Default classification for unknown symbols (assume US stock)
    return {
        'asset_class': 'Stocks',
        'sub_class': 'US Large Cap',
        'sector': 'Unknown',
        'geography': 'US'
    }


def calculate_allocations(positions):
    """Calculate asset allocation, sector exposure, and geographic breakdown."""
    total_value = sum(p.get('value', 0) or 0 for p in positions)

    if total_value == 0:
        return {
            'asset_allocation': {},
            'sub_class_allocation': {},
            'sector_exposure': {},
            'geography': {},
            'total_value': 0
        }

    asset_allocation = {}
    sub_class_allocation = {}
    sector_exposure = {}
    geography = {}

    for pos in positions:
        value = pos.get('value', 0) or 0
        if value <= 0:
            continue

        symbol = pos.get('symbol', '')
        classification = get_classification(symbol)

        asset_class = classification['asset_class']
        sub_class = classification['sub_class']
        sector = classification['sector']
        geo = classification['geography']

        # Aggregate by asset class
        asset_allocation[asset_class] = asset_allocation.get(asset_class, 0) + value

        # Aggregate by sub-class
        sub_class_allocation[sub_class] = sub_class_allocation.get(sub_class, 0) + value

        # Aggregate by sector
        sector_exposure[sector] = sector_exposure.get(sector, 0) + value

        # Aggregate by geography
        geography[geo] = geography.get(geo, 0) + value

    # Convert to percentages
    asset_pct = {k: round(v / total_value * 100, 2) for k, v in asset_allocation.items()}
    sub_class_pct = {k: round(v / total_value * 100, 2) for k, v in sub_class_allocation.items()}
    sector_pct = {k: round(v / total_value * 100, 2) for k, v in sector_exposure.items()}
    geo_pct = {k: round(v / total_value * 100, 2) for k, v in geography.items()}

    return {
        'asset_allocation': asset_pct,
        'asset_allocation_values': {k: round(v, 2) for k, v in asset_allocation.items()},
        'sub_class_allocation': sub_class_pct,
        'sector_exposure': sector_pct,
        'sector_benchmark': SP500_SECTOR_WEIGHTS,
        'geography': geo_pct,
        'total_value': round(total_value, 2)
    }


def calculate_concentration(positions):
    """Calculate concentration risk - top 10 holdings percentage."""
    total_value = sum(p.get('value', 0) or 0 for p in positions)

    if total_value == 0:
        return {'top_10_pct': 0, 'top_10_holdings': []}

    # Sort positions by value descending
    sorted_positions = sorted(
        [p for p in positions if (p.get('value', 0) or 0) > 0],
        key=lambda x: x.get('value', 0) or 0,
        reverse=True
    )

    top_10 = sorted_positions[:10]
    top_10_value = sum(p.get('value', 0) or 0 for p in top_10)

    top_10_holdings = [
        {
            'symbol': p.get('symbol', ''),
            'value': round(p.get('value', 0) or 0, 2),
            'pct': round((p.get('value', 0) or 0) / total_value * 100, 2)
        }
        for p in top_10
    ]

    return {
        'top_10_pct': round(top_10_value / total_value * 100, 2),
        'top_10_holdings': top_10_holdings
    }


def calculate_risk_metrics(positions):
    """Calculate portfolio risk metrics using historical data."""
    if not YFINANCE_AVAILABLE:
        return {
            'volatility': None,
            'beta': None,
            'sharpe_ratio': None,
            'max_drawdown': None,
            'error': 'yfinance not available'
        }

    total_value = sum(p.get('value', 0) or 0 for p in positions)
    if total_value == 0:
        return {
            'volatility': None,
            'beta': None,
            'sharpe_ratio': None,
            'max_drawdown': None
        }

    # Get symbols with weights
    weights = {}
    for pos in positions:
        symbol = pos.get('symbol', '')
        value = pos.get('value', 0) or 0
        if value > 0 and symbol and symbol != 'CASH':
            weights[symbol] = value / total_value

    if not weights:
        return {
            'volatility': None,
            'beta': None,
            'sharpe_ratio': None,
            'max_drawdown': None
        }

    try:
        # Get 1 year of historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        symbols = list(weights.keys())

        # Download price data
        data = yf.download(
            symbols + ['SPY'],  # Include SPY for beta calculation
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True
        )['Close']

        if data.empty:
            return {
                'volatility': None,
                'beta': None,
                'sharpe_ratio': None,
                'max_drawdown': None,
                'error': 'No price data available'
            }

        # Handle single symbol case
        if len(symbols) == 1:
            data = data.to_frame()
            data.columns = [symbols[0]]

        # Calculate daily returns
        returns = data.pct_change().dropna()

        if returns.empty:
            return {
                'volatility': None,
                'beta': None,
                'sharpe_ratio': None,
                'max_drawdown': None
            }

        # Calculate portfolio returns
        portfolio_returns = pd.Series(0, index=returns.index)
        for symbol, weight in weights.items():
            if symbol in returns.columns:
                portfolio_returns += returns[symbol] * weight

        # Annualized volatility (std dev)
        volatility = float(portfolio_returns.std() * np.sqrt(252) * 100)

        # Beta vs S&P 500
        if 'SPY' in returns.columns:
            covariance = portfolio_returns.cov(returns['SPY'])
            spy_variance = returns['SPY'].var()
            beta = float(covariance / spy_variance) if spy_variance > 0 else None
        else:
            beta = None

        # Sharpe Ratio (assuming 5% risk-free rate)
        risk_free_rate = 0.05
        excess_returns = portfolio_returns.mean() * 252 - risk_free_rate
        sharpe = float(excess_returns / (portfolio_returns.std() * np.sqrt(252))) if portfolio_returns.std() > 0 else None

        # Max Drawdown
        cumulative = (1 + portfolio_returns).cumprod()
        rolling_max = cumulative.cummax()
        drawdown = (cumulative - rolling_max) / rolling_max
        max_drawdown = float(drawdown.min() * 100)

        return {
            'volatility': round(volatility, 2),
            'beta': round(beta, 2) if beta is not None else None,
            'sharpe_ratio': round(sharpe, 2) if sharpe is not None else None,
            'max_drawdown': round(max_drawdown, 2)
        }

    except Exception as e:
        return {
            'volatility': None,
            'beta': None,
            'sharpe_ratio': None,
            'max_drawdown': None,
            'error': str(e)
        }


def calculate_historical_performance(positions):
    """Calculate historical returns and performance chart data."""
    if not YFINANCE_AVAILABLE:
        return {
            'returns': {},
            'chart_data': None,
            'error': 'yfinance not available'
        }

    total_value = sum(p.get('value', 0) or 0 for p in positions)
    if total_value == 0:
        return {'returns': {}, 'chart_data': None}

    # Get symbols with weights
    weights = {}
    for pos in positions:
        symbol = pos.get('symbol', '')
        value = pos.get('value', 0) or 0
        if value > 0 and symbol and symbol != 'CASH':
            weights[symbol] = value / total_value

    # Include cash weight for accurate returns
    cash_weight = 0
    for pos in positions:
        if pos.get('symbol', '') == 'CASH':
            cash_weight = (pos.get('value', 0) or 0) / total_value

    if not weights:
        return {'returns': {}, 'chart_data': None}

    try:
        # Get 1 year of historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=400)  # Extra days for YTD calculation

        symbols = list(weights.keys())

        # Download price data
        data = yf.download(
            symbols + ['SPY'],
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True
        )['Close']

        if data.empty:
            return {'returns': {}, 'chart_data': None, 'error': 'No price data'}

        # Handle single symbol case
        if len(symbols) == 1 and 'SPY' not in symbols:
            data = data.to_frame()
            data.columns = [symbols[0]]
            # Re-download with SPY
            spy_data = yf.download('SPY', start=start_date, end=end_date, progress=False, auto_adjust=True)['Close']
            data['SPY'] = spy_data

        # Calculate daily returns
        returns = data.pct_change().dropna()

        if returns.empty:
            return {'returns': {}, 'chart_data': None}

        # Calculate portfolio returns (weighted)
        portfolio_returns = pd.Series(0.0, index=returns.index)
        for symbol, weight in weights.items():
            if symbol in returns.columns:
                portfolio_returns += returns[symbol] * weight
        # Cash portion earns ~5% annual (approximate money market rate)
        if cash_weight > 0:
            daily_cash_return = (1.05 ** (1/252)) - 1
            portfolio_returns += cash_weight * daily_cash_return

        # Calculate cumulative returns for chart
        portfolio_cumulative = (1 + portfolio_returns).cumprod()
        spy_cumulative = (1 + returns['SPY']).cumprod() if 'SPY' in returns.columns else None

        # Calculate period returns
        today = returns.index[-1]
        period_returns = {}

        # Helper to calculate return over period
        def calc_return(days_back):
            target_date = today - timedelta(days=days_back)
            # Find closest available date
            valid_dates = portfolio_cumulative.index[portfolio_cumulative.index <= target_date]
            if len(valid_dates) == 0:
                return None, None
            start_idx = valid_dates[-1]
            port_ret = (portfolio_cumulative.iloc[-1] / portfolio_cumulative.loc[start_idx] - 1) * 100
            spy_ret = None
            if spy_cumulative is not None:
                spy_ret = (spy_cumulative.iloc[-1] / spy_cumulative.loc[start_idx] - 1) * 100
            return round(float(port_ret), 2), round(float(spy_ret), 2) if spy_ret is not None else None

        # 1 Month
        port_1m, spy_1m = calc_return(30)
        period_returns['1M'] = {'portfolio': port_1m, 'benchmark': spy_1m}

        # 3 Month
        port_3m, spy_3m = calc_return(90)
        period_returns['3M'] = {'portfolio': port_3m, 'benchmark': spy_3m}

        # 6 Month
        port_6m, spy_6m = calc_return(180)
        period_returns['6M'] = {'portfolio': port_6m, 'benchmark': spy_6m}

        # 1 Year
        port_1y, spy_1y = calc_return(365)
        period_returns['1Y'] = {'portfolio': port_1y, 'benchmark': spy_1y}

        # YTD
        year_start = datetime(today.year, 1, 1)
        valid_dates = portfolio_cumulative.index[portfolio_cumulative.index >= year_start]
        if len(valid_dates) > 0:
            start_idx = valid_dates[0]
            port_ytd = (portfolio_cumulative.iloc[-1] / portfolio_cumulative.loc[start_idx] - 1) * 100
            spy_ytd = None
            if spy_cumulative is not None:
                spy_ytd = (spy_cumulative.iloc[-1] / spy_cumulative.loc[start_idx] - 1) * 100
            period_returns['YTD'] = {
                'portfolio': round(float(port_ytd), 2),
                'benchmark': round(float(spy_ytd), 2) if spy_ytd is not None else None
            }
        else:
            period_returns['YTD'] = {'portfolio': None, 'benchmark': None}

        # Generate chart data (last 1 year, weekly points for performance)
        one_year_ago = today - timedelta(days=365)
        chart_mask = portfolio_cumulative.index >= one_year_ago

        # Resample to weekly for smoother chart
        portfolio_weekly = portfolio_cumulative[chart_mask]
        spy_weekly = spy_cumulative[chart_mask] if spy_cumulative is not None else None

        # Normalize to start at 100
        if len(portfolio_weekly) > 0:
            portfolio_normalized = (portfolio_weekly / portfolio_weekly.iloc[0]) * 100
            chart_data = {
                'labels': [d.strftime('%Y-%m-%d') for d in portfolio_normalized.index],
                'portfolio': [round(float(v), 2) for v in portfolio_normalized.values],
            }
            if spy_weekly is not None and len(spy_weekly) > 0:
                spy_normalized = (spy_weekly / spy_weekly.iloc[0]) * 100
                chart_data['benchmark'] = [round(float(v), 2) for v in spy_normalized.values]
        else:
            chart_data = None

        return {
            'returns': period_returns,
            'chart_data': chart_data
        }

    except Exception as e:
        return {
            'returns': {},
            'chart_data': None,
            'error': str(e)
        }


@app.route('/analyze', methods=['POST'])
def analyze_portfolio():
    """Analyze a portfolio and return comprehensive analytics."""
    try:
        data = request.get_json()

        if not data or 'positions' not in data:
            return jsonify({'error': 'No positions provided'}), 400

        positions = data['positions']

        if not positions:
            return jsonify({'error': 'Empty positions list'}), 400

        # Calculate all analytics
        allocations = calculate_allocations(positions)
        concentration = calculate_concentration(positions)

        # Risk metrics (optional, can be slow)
        include_risk = data.get('include_risk', True)
        if include_risk:
            risk_metrics = calculate_risk_metrics(positions)
            historical_performance = calculate_historical_performance(positions)
        else:
            risk_metrics = {
                'volatility': None,
                'beta': None,
                'sharpe_ratio': None,
                'max_drawdown': None
            }
            historical_performance = {'returns': {}, 'chart_data': None}

        # Add classification to each position
        classified_positions = []
        for pos in positions:
            classified_pos = pos.copy()
            classification = get_classification(pos.get('symbol', ''))
            classified_pos['classification'] = classification
            classified_positions.append(classified_pos)

        return jsonify({
            'positions': classified_positions,
            'total_value': allocations['total_value'],
            'asset_allocation': allocations['asset_allocation'],
            'asset_allocation_values': allocations['asset_allocation_values'],
            'sub_class_allocation': allocations['sub_class_allocation'],
            'sector_exposure': allocations['sector_exposure'],
            'sector_benchmark': allocations['sector_benchmark'],
            'geography': allocations['geography'],
            'concentration': concentration,
            'risk_metrics': risk_metrics,
            'historical_performance': historical_performance
        })

    except Exception as e:
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
