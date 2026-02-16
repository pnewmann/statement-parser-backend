"""
Brokerage Statement Parser API - Enterprise Edition
Extracts positions from Schwab, Fidelity, and other brokerage statements.
Provides portfolio analytics including asset allocation, sector exposure, and risk metrics.
Includes user authentication, portfolio saving, and Plaid integration.
"""

import io
import csv
import re
import os
import secrets
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required,
    get_jwt_identity, verify_jwt_in_request
)
import bcrypt
import pdfplumber

# OCR is now handled client-side with Tesseract.js
# Keeping PIL for any image processing needs
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

OCR_AVAILABLE = False  # OCR moved to client-side
OCR_READER = None

from models import db, User, Portfolio, PlaidConnection
from plaid_client import plaid_client

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

# Try to import weasyprint for PDF generation
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

app = Flask(__name__)

# Database configuration
database_url = os.environ.get('DATABASE_URL', 'sqlite:///statement_scan.db')
# Handle Render's postgres:// URL (SQLAlchemy requires postgresql+psycopg://)
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql+psycopg://', 1)
elif database_url.startswith('postgresql://'):
    database_url = database_url.replace('postgresql://', 'postgresql+psycopg://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# JWT configuration
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=24)

# Initialize extensions
db.init_app(app)
jwt = JWTManager(app)
CORS(app)

# Create tables on first request if they don't exist
with app.app_context():
    db.create_all()


def optional_jwt_required():
    """Decorator that allows optional JWT authentication."""
    def wrapper(fn):
        @wraps(fn)
        def decorator(*args, **kwargs):
            try:
                verify_jwt_in_request(optional=True)
            except Exception:
                pass
            return fn(*args, **kwargs)
        return decorator
    return wrapper

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
    # DIRECT CRYPTOCURRENCY (Robinhood, Coinbase, etc.)
    # =========================================================================
    'BTC': {'asset_class': 'Crypto', 'sub_class': 'Bitcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'ETH': {'asset_class': 'Crypto', 'sub_class': 'Ethereum', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'SOL': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'DOGE': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'ADA': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'XRP': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'DOT': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'AVAX': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'MATIC': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'LINK': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'LTC': {'asset_class': 'Crypto', 'sub_class': 'Altcoin', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'UNI': {'asset_class': 'Crypto', 'sub_class': 'DeFi', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'AAVE': {'asset_class': 'Crypto', 'sub_class': 'DeFi', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'SHIB': {'asset_class': 'Crypto', 'sub_class': 'Meme', 'sector': 'Cryptocurrency', 'geography': 'Global'},
    'PEPE': {'asset_class': 'Crypto', 'sub_class': 'Meme', 'sector': 'Cryptocurrency', 'geography': 'Global'},

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
    # Fidelity Money Market Funds
    'SPAXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'FDRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'FZFXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'SPRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'FDLXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'FTEXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # Vanguard Money Market Funds
    'VMFXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'VMMXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'VUSXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # Schwab Money Market Funds
    'SWVXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'SNOXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'SNVXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # Morgan Stanley Money Market Funds
    'MVRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'MOFXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'MPRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'MCIXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'MISXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'MPUXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # TD Ameritrade / Other Money Market Funds
    'TDFXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'TTTXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'GVMXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'Dgovt': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # E*TRADE Money Market Funds
    'ETGXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # Merrill Lynch / Bank of America
    'MLAXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # JP Morgan Money Market Funds
    'MJPXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'JPGXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'CJPXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    'OGVXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # Goldman Sachs Money Market Funds
    'FGTXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # American Funds Money Market
    'AFAXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},
    # T. Rowe Price
    'PRRXX': {'asset_class': 'Cash', 'sub_class': 'Money Market', 'sector': 'Money Market', 'geography': 'US'},

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
    'BLK': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    'SCHW': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Financials', 'geography': 'US'},
    # GSEs / Mortgage
    'FNMA': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Financials', 'geography': 'US'},
    'FMCC': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Financials', 'geography': 'US'},

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
    # Uranium / Nuclear Energy
    'UEC': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Energy', 'geography': 'US'},
    'DNN': {'asset_class': 'Stocks', 'sub_class': 'International Small Cap', 'sector': 'Energy', 'geography': 'Canada'},
    'NXE': {'asset_class': 'Stocks', 'sub_class': 'International Small Cap', 'sector': 'Energy', 'geography': 'Canada'},
    'CCJ': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Energy', 'geography': 'US'},
    'UUUU': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Energy', 'geography': 'US'},
    'LEU': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Energy', 'geography': 'US'},

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
    # Packaging / Materials
    'GPK': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Materials', 'geography': 'US'},
    'PKG': {'asset_class': 'Stocks', 'sub_class': 'US Mid Cap', 'sector': 'Materials', 'geography': 'US'},
    'IP': {'asset_class': 'Stocks', 'sub_class': 'US Large Cap', 'sector': 'Materials', 'geography': 'US'},
    # Tech / Speculative
    'AEVA': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Technology', 'geography': 'US'},
    'LAZR': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Technology', 'geography': 'US'},
    'MVIS': {'asset_class': 'Stocks', 'sub_class': 'US Small Cap', 'sector': 'Technology', 'geography': 'US'},

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
    # Direct Crypto
    'BTC', 'ETH', 'SOL', 'DOGE', 'ADA', 'XRP', 'DOT', 'AVAX', 'MATIC', 'LINK',
    'LTC', 'UNI', 'AAVE', 'SHIB', 'PEPE',
    # Sector ETFs
    'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE',
    'VGT', 'VFH', 'VDE', 'VHT', 'VIS', 'VCR', 'VDC', 'VPU', 'VAW', 'VNQ',
    # Individual stocks
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK',
    'JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'VZ',
    'ADBE', 'NFLX', 'CRM', 'PFE', 'TMO', 'PEP', 'AVGO', 'CSCO', 'ACN',
    'WMT', 'KO', 'MRK', 'ABT', 'CVX', 'XOM', 'LLY', 'ABBV', 'ORCL', 'AMD',
    'INTC', 'QCOM', 'TXN', 'IBM', 'GE', 'CAT', 'BA', 'MMM', 'HON', 'UPS',
    # Asset managers, financials
    'BLK', 'SCHW', 'GS', 'MS', 'C', 'WFC', 'USB', 'PNC', 'TFC', 'BK', 'AXP',
    # Energy/Uranium stocks
    'UEC', 'DNN', 'NXE', 'CCJ', 'UUUU', 'URG', 'LEU', 'SMR',
    # GSEs and mortgage companies
    'FMCC', 'FNMA', 'AGNC', 'NLY', 'STWD', 'BXMT',
    # Consumer/packaging
    'GPK', 'PKG', 'IP', 'WRK', 'SEE', 'BLL', 'CCK',
    # Tech/EV/speculative
    'AEVA', 'LAZR', 'VLDR', 'INVZ', 'OUST', 'CPTN', 'MVIS', 'LIDR',
    # ARK ETFs
    'ARKK', 'ARKW', 'ARKG', 'ARKF', 'ARKQ', 'ARKX', 'PRNT', 'IZRL',
    # Stifel mutual funds
    'COSZX', 'AEPFX', 'FCDIX', 'GFFFX', 'GIBIX', 'LAPIX', 'NFFFX', 'PIGIX',
    'PIMIX', 'PCBIX', 'VSIGX', 'WMFFX',
    # Vanguard Institutional funds (retirement plans, 401k)
    'VBTIX', 'VINIX', 'VTSNX', 'VTIAX', 'VTSAX', 'VFIAX', 'VBTLX', 'VBIAX',
    'VTABX', 'VWENX', 'VWELX', 'VPMAX', 'VWUAX', 'VWINX', 'VGSNX', 'VIPIX',
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

# =============================================================================
# CLEARING FIRM FINGERPRINTS
# Many brokerages use clearing firms which have standardized statement formats
# =============================================================================

CLEARING_FIRM_FINGERPRINTS = {
    'pershing': {
        'patterns': [
            'pershing llc', 'pershing advisor solutions', 'bny mellon',
            'pershing x', 'pershing netx360', 'clearing through pershing'
        ],
        'statement_format': 'pershing',
        'brokerages': ['advisor_generic', 'ria_generic']
    },
    'nfs': {  # National Financial Services (Fidelity's clearing arm)
        'patterns': [
            'national financial services', 'nfs llc', 'fidelity clearing',
            'cleared through nfs', 'fidelity brokerage services'
        ],
        'statement_format': 'fidelity',
        'brokerages': ['fidelity', 'advisor_generic']
    },
    'apex': {
        'patterns': [
            'apex clearing', 'apex clearing corporation', 'cleared by apex',
            'apex fintech solutions'
        ],
        'statement_format': 'apex',
        'brokerages': ['webull', 'sofi', 'public', 'tastyworks', 'm1_finance', 'firstrade']
    },
    'schwab': {
        'patterns': [
            'charles schwab & co', 'schwab clearing', 'td ameritrade clearing'
        ],
        'statement_format': 'schwab',
        'brokerages': ['schwab', 'tdameritrade']
    },
    'rqd': {  # Raymond James
        'patterns': [
            'raymond james', 'rj & associates', 'raymond james financial'
        ],
        'statement_format': 'raymond_james',
        'brokerages': ['raymond_james']
    },
    'vanguard': {
        'patterns': [
            'vanguard brokerage services', 'vanguard marketing corporation'
        ],
        'statement_format': 'vanguard',
        'brokerages': ['vanguard']
    },
    'interactive_brokers': {
        'patterns': [
            'interactive brokers llc', 'ibkr', 'ib llc', 'timber hill'
        ],
        'statement_format': 'ibkr',
        'brokerages': ['interactive_brokers']
    }
}

# Enhanced column aliases for CSV parsing
COLUMN_ALIASES = {
    'symbol': [
        'symbol', 'ticker', 'sym', 'stock symbol', 'security symbol',
        'instrument', 'ticker symbol', 'fund symbol', 'etf symbol',
        'financial instrument', 'cusip', 'isin', 'sedol'
    ],
    'description': [
        'description', 'security', 'name', 'security name', 'security description',
        'company', 'company name', 'fund name', 'holding name', 'instrument name',
        'investment', 'investment name', 'asset', 'asset name', 'position',
        'holding description', 'security desc'
    ],
    'shares': [
        'quantity', 'shares', 'units', 'qty', 'holdings', 'position',
        'share balance', 'unit balance', 'units owned', 'shares owned',
        'quantity held', 'balance', 'units held', 'share quantity',
        'current quantity', 'ending quantity', 'total shares', 'total units',
        'closing balance', 'ending balance'
    ],
    'price': [
        'price', 'last price', 'closing price', 'market price', 'current price',
        'unit price', 'share price', 'last', 'close', 'closing', 'mkt price',
        'price per share', 'nav', 'net asset value', 'quote', 'last quote',
        'price/share', 'cost basis per share'
    ],
    'value': [
        'value', 'market value', 'total value', 'current value', 'amount',
        'balance', 'market val', 'mkt value', 'mkt val', 'position value',
        'total market value', 'ending value', 'account value', 'fair value',
        'fmv', 'fair market value', 'worth', 'total', 'total amount',
        'ending market value', 'current market value'
    ],
    'cost_basis': [
        'cost basis', 'cost', 'average cost', 'avg cost', 'purchase price',
        'acquisition cost', 'book value', 'tax basis', 'total cost',
        'original cost', 'cost per share', 'unit cost', 'adjusted cost basis'
    ],
    'gain_loss': [
        'gain/loss', 'gain loss', 'unrealized gain', 'unrealized loss',
        'unrealized g/l', 'p&l', 'profit loss', 'change', 'total gain',
        'total return', 'unrealized p/l', 'return', 'appreciation'
    ],
    'account': [
        'account', 'account number', 'acct', 'account #', 'acct #',
        'account id', 'portfolio', 'account name', 'registration'
    ],
    'sector': [
        'sector', 'industry', 'gics sector', 'industry sector',
        'classification', 'asset class', 'category', 'type'
    ]
}

# CUSIP to ticker mapping for common securities
# This helps when statements only provide CUSIP codes
COMMON_CUSIPS = {
    # Popular ETFs
    '78462F103': 'SPY',    # SPDR S&P 500
    '922908363': 'VTI',    # Vanguard Total Stock Market
    '922908769': 'VOO',    # Vanguard S&P 500
    '46434V100': 'IVV',    # iShares Core S&P 500
    '464287200': 'IJH',    # iShares Core S&P Mid-Cap
    '464287622': 'IJR',    # iShares Core S&P Small-Cap
    '78464A870': 'QQQ',    # Invesco QQQ Trust
    '922908355': 'VEA',    # Vanguard FTSE Developed Markets
    '922908793': 'VWO',    # Vanguard FTSE Emerging Markets
    '922908645': 'BND',    # Vanguard Total Bond Market
    '46434V407': 'AGG',    # iShares Core US Aggregate Bond
    '922908728': 'VXUS',   # Vanguard Total International Stock
    '921937835': 'VNQ',    # Vanguard Real Estate
    '78468R101': 'SCHD',   # Schwab US Dividend Equity
    '46090E103': 'IEFA',   # iShares Core MSCI EAFE
    '46434G103': 'IEMG',   # iShares Core MSCI Emerging Markets

    # Popular stocks
    '037833100': 'AAPL',   # Apple
    '594918104': 'MSFT',   # Microsoft
    '02079K305': 'GOOGL',  # Alphabet Class A
    '02079K107': 'GOOG',   # Alphabet Class C
    '023135106': 'AMZN',   # Amazon
    '88160R101': 'TSLA',   # Tesla
    '30303M102': 'META',   # Meta Platforms
    '67066G104': 'NVDA',   # NVIDIA
    '11135F101': 'AVGO',   # Broadcom
    '46625H100': 'JPM',    # JPMorgan Chase
    '46647P100': 'JNJ',    # Johnson & Johnson
    '91324P102': 'UNH',    # UnitedHealth
    '94106L109': 'WMT',    # Walmart
    '5949181045': 'MSFT',  # Microsoft (alternate)
    '30231G102': 'XOM',    # Exxon Mobil
    '78378X107': 'CVX',    # Chevron
    '713448108': 'PEP',    # PepsiCo
    '191216100': 'KO',     # Coca-Cola
    '742718109': 'PG',     # Procter & Gamble
    '172967424': 'C',      # Citigroup
    '060505104': 'BAC',    # Bank of America
    '38141G104': 'GS',     # Goldman Sachs
}


def detect_clearing_firm(text):
    """Detect which clearing firm processed the statement."""
    text_lower = text.lower()

    for firm_id, firm_data in CLEARING_FIRM_FINGERPRINTS.items():
        for pattern in firm_data['patterns']:
            if pattern in text_lower:
                return {
                    'clearing_firm': firm_id,
                    'statement_format': firm_data['statement_format'],
                    'possible_brokerages': firm_data['brokerages']
                }

    return None


def cusip_to_ticker(cusip):
    """Convert CUSIP to ticker symbol if known."""
    if not cusip:
        return None
    # Remove any whitespace and standardize
    cusip = cusip.strip().upper().replace(' ', '').replace('-', '')
    # CUSIPs are 9 characters, but some formats omit the check digit
    if len(cusip) == 8:
        # Try both with and without check digit
        for suffix in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            full_cusip = cusip + suffix
            if full_cusip in COMMON_CUSIPS:
                return COMMON_CUSIPS[full_cusip]
    return COMMON_CUSIPS.get(cusip)


def match_column(header, column_type):
    """Check if a header matches any alias for a column type."""
    if column_type not in COLUMN_ALIASES:
        return False
    header_lower = header.lower().strip()
    for alias in COLUMN_ALIASES[column_type]:
        if alias in header_lower or header_lower in alias:
            return True
    return False


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


# =============================================================================
# OFX/QFX PARSER
# Open Financial Exchange format used by many brokerages for downloads
# =============================================================================

def parse_ofx_file(content):
    """
    Parse OFX/QFX file format (Open Financial Exchange).
    This format is commonly offered as a download option by:
    - Vanguard, Fidelity, Schwab, E*TRADE, TD Ameritrade
    - Quicken-compatible exports from most brokerages
    """
    positions = []
    brokerage = 'ofx'

    if isinstance(content, bytes):
        # Try different encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                content = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue

    # Detect brokerage from OFX header
    content_lower = content.lower()
    if 'vanguard' in content_lower:
        brokerage = 'vanguard'
    elif 'fidelity' in content_lower:
        brokerage = 'fidelity'
    elif 'schwab' in content_lower:
        brokerage = 'schwab'
    elif 'ameritrade' in content_lower:
        brokerage = 'tdameritrade'
    elif 'e*trade' in content_lower or 'etrade' in content_lower:
        brokerage = 'etrade'

    # Parse stock positions from INVPOSLIST
    # OFX uses SGML-like tags
    pos_pattern = r'<POSSTOCK>(.*?)</POSSTOCK>|<POSMF>(.*?)</POSMF>|<POSOTHER>(.*?)</POSOTHER>|<POSOPT>(.*?)</POSOPT>'
    pos_matches = re.findall(pos_pattern, content, re.IGNORECASE | re.DOTALL)

    for match_tuple in pos_matches:
        # Get the non-empty match from the tuple
        pos_content = next((m for m in match_tuple if m), '')
        if not pos_content:
            continue

        position = {}

        # Extract SECID (security identifier)
        secid_match = re.search(r'<SECID>(.*?)</SECID>', pos_content, re.IGNORECASE | re.DOTALL)
        if secid_match:
            secid_content = secid_match.group(1)
            # Get UNIQUEID (usually CUSIP)
            uniqueid = re.search(r'<UNIQUEID>([^<]+)', secid_content, re.IGNORECASE)
            uniqueidtype = re.search(r'<UNIQUEIDTYPE>([^<]+)', secid_content, re.IGNORECASE)

            if uniqueid:
                cusip_or_ticker = uniqueid.group(1).strip()
                id_type = uniqueidtype.group(1).strip().upper() if uniqueidtype else 'CUSIP'

                if id_type == 'CUSIP':
                    # Try to convert CUSIP to ticker
                    ticker = cusip_to_ticker(cusip_or_ticker)
                    if ticker:
                        position['symbol'] = ticker
                    else:
                        position['symbol'] = cusip_or_ticker  # Use CUSIP as fallback
                        position['cusip'] = cusip_or_ticker
                else:
                    position['symbol'] = cusip_or_ticker

        # Extract units/shares
        units_match = re.search(r'<UNITS>([^<]+)', pos_content, re.IGNORECASE)
        if units_match:
            position['shares'] = clean_number(units_match.group(1))

        # Extract unit price
        price_match = re.search(r'<UNITPRICE>([^<]+)', pos_content, re.IGNORECASE)
        if price_match:
            position['price'] = clean_number(price_match.group(1))

        # Extract market value
        mktval_match = re.search(r'<MKTVAL>([^<]+)', pos_content, re.IGNORECASE)
        if mktval_match:
            position['value'] = clean_number(mktval_match.group(1))

        # Calculate value if not present
        if not position.get('value') and position.get('shares') and position.get('price'):
            position['value'] = position['shares'] * position['price']

        if position.get('symbol') and (position.get('shares') or position.get('value')):
            position['description'] = position.get('symbol', '')
            positions.append(position)

    # Also look for security info to get descriptions
    seclist_pattern = r'<SECINFO>(.*?)</SECINFO>'
    sec_matches = re.findall(seclist_pattern, content, re.IGNORECASE | re.DOTALL)

    sec_descriptions = {}
    for sec_content in sec_matches:
        uniqueid = re.search(r'<UNIQUEID>([^<]+)', sec_content, re.IGNORECASE)
        secname = re.search(r'<SECNAME>([^<]+)', sec_content, re.IGNORECASE)
        ticker = re.search(r'<TICKER>([^<]+)', sec_content, re.IGNORECASE)

        if uniqueid:
            uid = uniqueid.group(1).strip()
            sec_descriptions[uid] = {
                'name': secname.group(1).strip() if secname else '',
                'ticker': ticker.group(1).strip() if ticker else ''
            }

    # Update positions with descriptions and tickers from SECLIST
    for pos in positions:
        symbol = pos.get('symbol', '')
        cusip = pos.get('cusip', symbol)

        if cusip in sec_descriptions:
            sec_info = sec_descriptions[cusip]
            if sec_info['name']:
                pos['description'] = sec_info['name']
            if sec_info['ticker'] and not pos.get('symbol'):
                pos['symbol'] = sec_info['ticker']
            elif sec_info['ticker']:
                # Replace CUSIP with actual ticker if available
                pos['symbol'] = sec_info['ticker']

    return positions, brokerage


def detect_brokerage_pdf(text):
    """Detect which brokerage the PDF is from using multiple fingerprints."""
    text_lower = text.lower()

    # Check for clearing firm first (helps identify statement format)
    clearing_info = detect_clearing_firm(text)

    # Check Acropolis/retirement plans FIRST (they contain Vanguard fund names)
    if 'acropolis' in text_lower or ('profit sharing plan' in text_lower and 'your market value' in text_lower):
        return 'acropolis'

    # Major brokerages
    if 'charles schwab' in text_lower or 'schwab' in text_lower:
        return 'schwab'
    if 'fidelity' in text_lower or 'fmr llc' in text_lower:
        return 'fidelity'
    if 'vanguard' in text_lower:
        return 'vanguard'
    if 'td ameritrade' in text_lower or 'thinkorswim' in text_lower:
        return 'tdameritrade'
    if 'e*trade' in text_lower or 'etrade' in text_lower:
        return 'etrade'
    if 'robinhood' in text_lower:
        return 'robinhood'
    if 'stifel' in text_lower or 'lefits' in text_lower:
        return 'stifel'
    if 'morgan stanley' in text_lower:
        return 'morgan_stanley'

    # Additional brokerages with fingerprints
    if 'interactive brokers' in text_lower or 'ibkr' in text_lower:
        return 'interactive_brokers'
    if 'merrill' in text_lower or 'merrill lynch' in text_lower or 'merrill edge' in text_lower:
        return 'merrill'
    if 'wells fargo' in text_lower or 'wellstrade' in text_lower:
        return 'wells_fargo'
    if 'webull' in text_lower:
        return 'webull'
    if 'sofi' in text_lower or 'social finance' in text_lower:
        return 'sofi'
    if 'public.com' in text_lower or 'public investing' in text_lower:
        return 'public'
    if 'tastyworks' in text_lower or 'tastytrade' in text_lower:
        return 'tastyworks'
    if 'm1 finance' in text_lower or 'm1finance' in text_lower:
        return 'm1_finance'
    if 'firstrade' in text_lower:
        return 'firstrade'
    if 'ally invest' in text_lower:
        return 'ally'
    if 'betterment' in text_lower:
        return 'betterment'
    if 'wealthfront' in text_lower:
        return 'wealthfront'
    if 'raymond james' in text_lower:
        return 'raymond_james'
    if 'edward jones' in text_lower:
        return 'edward_jones'
    if 'ubs' in text_lower and ('financial' in text_lower or 'wealth' in text_lower):
        return 'ubs'
    if 'jp morgan' in text_lower or 'jpmorgan' in text_lower or 'chase private client' in text_lower:
        return 'jpmorgan'
    if 'goldman sachs' in text_lower or 'marcus' in text_lower:
        return 'goldman'
    if 'citibank' in text_lower or 'citi personal wealth' in text_lower:
        return 'citi'
    if 'usaa' in text_lower:
        return 'usaa'
    if 'tiaa' in text_lower:
        return 'tiaa'

    # Apex clearing firm brokerages (common format)
    if clearing_info and clearing_info['clearing_firm'] == 'apex':
        return 'apex_cleared'

    # Pershing clearing firm (common RIA format)
    if clearing_info and clearing_info['clearing_firm'] == 'pershing':
        return 'pershing_cleared'

    return 'unknown'


def parse_schwab_pdf(pdf):
    """Parse Charles Schwab brokerage statement using text extraction."""
    positions = []

    # Get all text from the PDF
    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    # Split into lines
    lines = full_text.split('\n')

    in_equities_section = False
    in_etf_section = False
    in_cash_section = False

    for line in lines:
        # Check for section headers
        if 'Positions - Equities' in line or ('Equities' in line and 'Symbol' not in line and 'Total' not in line):
            in_equities_section = True
            in_etf_section = False
            in_cash_section = False
            continue
        if 'Exchange Traded Funds' in line:
            in_etf_section = True
            in_equities_section = False
            in_cash_section = False
            continue
        if 'Cash and Cash Investments' in line:
            in_cash_section = True
            in_etf_section = False
            in_equities_section = False
            continue
        # Exit sections on these markers
        if 'Transaction' in line and 'Summary' in line:
            in_etf_section = False
            in_equities_section = False
            in_cash_section = False
            continue
        if 'Positions - Summary' in line:
            in_etf_section = False
            in_equities_section = False
            in_cash_section = False
            continue

        # Parse position from Equities or ETF sections
        if in_equities_section or in_etf_section:
            # Skip header lines
            if 'Symbol' in line and 'Description' in line:
                continue
            if 'Total' in line and ('Equities' in line or 'Exchange' in line or 'Traded' in line):
                in_equities_section = False
                in_etf_section = False
                continue

            # Try to match position line pattern
            # Format: SYMBOL DESCRIPTION QUANTITY PRICE MARKETVALUE ...
            # Example: BLK BLACKROCK INC NEW 16.0000 1,070.34000 17,125.44
            # Example: ARKK ARK INNOVATION ETF 62.4988 76.92000 4,807.41

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
                        first_num_match = re.search(r'[\d,]+\.[\d]+', line)
                        if first_num_match:
                            desc_end = first_num_match.start()
                            description = line[len(symbol):desc_end].strip()
                            # Clean up: remove special chars, trailing commas, (M) markers
                            description = re.sub(r'\s*\(M\)', '', description)
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
                    break

            # If no known symbol matched, try generic pattern
            if not matched:
                # Match: 2-5 letter symbol at start, followed by description and numbers
                match = re.match(r'^([A-Z]{2,5})\s+([A-Za-z0-9\-\s]+)', line)
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
                                # Clean up: remove (M) markers, special chars
                                description = re.sub(r'\s*\(M\)', '', description)
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
            # Look for cash line - can be "Cash" at start of line or include Schwab Bank
            line_stripped = line.strip()
            if line_stripped.startswith('Cash') or 'CHARLESSCHWAB' in line.replace(' ', '') or 'SCHWABBANK' in line.replace(' ', ''):
                if 'Total' in line or 'Investments' in line:
                    continue
                # Extract numbers - looking for ending balance
                numbers = re.findall(r'[\d,]+\.[\d]{2}', line)
                if len(numbers) >= 2:
                    # For "Cash , 1,489.55 1,520.27 ..." format, second number is ending balance
                    ending_balance = clean_number(numbers[1])
                    if ending_balance and ending_balance > 0:
                        if not any(p['symbol'] == 'CASH' for p in positions):
                            positions.append({
                                'symbol': 'CASH',
                                'description': 'Cash and Cash Investments',
                                'shares': None,
                                'price': None,
                                'value': round(ending_balance, 2)
                            })

    return positions


def parse_fidelity_pdf(pdf):
    """Parse Fidelity brokerage statement with proper section detection."""
    positions = []

    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    lines = full_text.split('\n')

    # Track which section we're in
    in_positions_section = False
    in_core_position = False

    # Fidelity section markers
    position_start_markers = [
        'STOCKS', 'EQUITIES', 'EXCHANGE-TRADED FUNDS', 'ETFs',
        'MUTUAL FUNDS', 'BONDS', 'FIXED INCOME', 'OPTIONS',
        'Your Holdings', 'Account Holdings', 'Investment Holdings',
        'Symbol', 'Description'  # Table headers
    ]
    position_end_markers = [
        'ACCOUNT ACTIVITY', 'TRANSACTION', 'DISCLOSURES',
        'Important Information', 'Account Features', 'Terms and Conditions',
        'This statement', 'Total Account Value'
    ]

    for i, line in enumerate(lines):
        line_upper = line.upper().strip()

        # Check for section start
        if any(marker.upper() in line_upper for marker in position_start_markers):
            in_positions_section = True
            continue

        # Check for section end
        if any(marker.upper() in line_upper for marker in position_end_markers):
            in_positions_section = False
            continue

        # Check for core position (cash/money market sweep)
        if 'CORE' in line_upper or 'FDIC' in line_upper or 'SPAXX' in line_upper:
            in_core_position = True

        if not in_positions_section and not in_core_position:
            # Still try to match clear position patterns outside sections
            pass

        # Try to match position line: SYMBOL at start followed by numbers
        match = re.match(r'^([A-Z]{1,5})\s+', line)
        if match:
            symbol = match.group(1)
            if is_valid_symbol(symbol):
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                if len(numbers) >= 2:
                    shares = clean_number(numbers[0])
                    value = clean_number(numbers[-1])

                    # Validate: shares should be reasonable
                    if shares and value and shares < 10000000 and value > 0:
                        position = {
                            'symbol': symbol,
                            'description': '',
                            'shares': shares,
                            'price': round(value / shares, 2) if shares > 0 else None,
                            'value': value
                        }

                        if not any(p['symbol'] == symbol for p in positions):
                            positions.append(position)
                        continue

        # Also check for known symbols mid-line (Fidelity format varies)
        if in_positions_section:
            for symbol in KNOWN_SYMBOLS:
                # Must be a word boundary match, not part of another word
                if re.search(rf'\b{symbol}\b', line):
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    if len(numbers) >= 2:
                        shares = clean_number(numbers[0])
                        value = clean_number(numbers[-1])

                        if shares and value and shares < 10000000:
                            position = {
                                'symbol': symbol,
                                'description': '',
                                'shares': shares,
                                'price': round(value / shares, 2) if shares > 0 else None,
                                'value': value
                            }

                            if not any(p['symbol'] == symbol for p in positions):
                                positions.append(position)
                    break

        in_core_position = False

    return positions


def parse_stifel_pdf(pdf):
    """Parse Stifel brokerage statement."""
    positions = []

    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    # Check if text is reversed (Stifel PDFs sometimes extract as reversed text)
    # If we see 'lefitS' instead of 'Stifel', the text is reversed
    if 'lefits' in full_text.lower() and 'stifel' not in full_text.lower():
        # Reverse each line's characters
        lines = full_text.split('\n')
        lines = [line[::-1] for line in lines]
    else:
        lines = full_text.split('\n')

    # Find each symbol and look at surrounding lines for numbers
    # Stifel format has symbol on its own line with data on preceding lines
    for i, line in enumerate(lines):
        line_stripped = line.strip()

        # Check if this line is a known symbol
        if line_stripped in KNOWN_SYMBOLS:
            symbol = line_stripped

            # Look at the 10 lines BEFORE the symbol (where data should be)
            context_lines = lines[max(0, i-10):i]

            # Extract numbers from each line (in reverse order to get closest first)
            numbers = []
            for ctx_line in reversed(context_lines):
                matches = re.findall(r'[\d,]+\.\d+', ctx_line.strip())
                for m in matches:
                    numbers.append(clean_number(m))

            if len(numbers) >= 3:
                # First number is typically shares (quantity)
                shares = numbers[0] if numbers[0] > 1 else None

                # Find price - typically between $5 and $150 for mutual funds
                price = None
                for n in numbers[1:5]:
                    if 5 <= n <= 150:
                        price = n
                        break

                if shares and price:
                    value = round(shares * price, 2)

                    position = {
                        'symbol': symbol,
                        'description': '',
                        'shares': shares,
                        'price': price,
                        'value': value
                    }

                    if not any(p['symbol'] == symbol for p in positions):
                        positions.append(position)

    return positions


def parse_acropolis_pdf(pdf):
    """Parse Acropolis Investment Management retirement plan statement.

    Acropolis statements (401k/Profit Sharing) have holdings in 'YOUR MARKET VALUE'
    section, with fund names that map to Vanguard institutional tickers.
    Format: Investment | Asset Class | Number of Shares | Price Per Share | Value | % Assets
    """
    positions = []

    # Fund name patterns to ticker mapping
    FUND_PATTERNS = [
        ('vanguard total bond market index', 'VBTIX'),
        ('vanguard institutional index', 'VINIX'),
        ('vanguard total intl stock index', 'VTSNX'),
        ('vanguard total international stock', 'VTSNX'),
        ('vanguard total stock market', 'VTSAX'),
        ('vanguard 500 index', 'VFIAX'),
        ('vanguard balanced index', 'VBIAX'),
        ('vanguard small cap index', 'VSCIX'),
        ('vanguard mid cap index', 'VMCIX'),
        ('vanguard developed markets', 'VTMGX'),
        ('vanguard real estate', 'VGSLX'),
    ]

    # First try table extraction on page 3 (index 2) where YOUR MARKET VALUE typically is
    for page_idx, page in enumerate(pdf.pages):
        tables = page.extract_tables()
        for table in tables:
            if not table:
                continue
            # Check if this looks like the market value table
            for row in table:
                if not row or len(row) < 4:
                    continue
                row_text = ' '.join(str(cell or '') for cell in row).lower()

                # Skip header rows
                if 'investment' in row_text and 'shares' in row_text:
                    continue
                if 'investment model' in row_text:
                    continue

                # Try to match fund patterns
                for fund_pattern, ticker in FUND_PATTERNS:
                    if fund_pattern in row_text:
                        # Extract numbers from the row
                        numbers = []
                        for cell in row:
                            if cell:
                                cell_nums = re.findall(r'[\d,]+\.[\d]+', str(cell))
                                for n in cell_nums:
                                    val = clean_number(n)
                                    if val is not None:
                                        numbers.append(val)

                        if len(numbers) >= 3:
                            # Format: Shares, Price, Value, Percent
                            shares = numbers[0]
                            price = numbers[1]
                            value = numbers[2]

                            # Validate shares * price ≈ value
                            if shares and price and value:
                                expected = shares * price
                                if abs(expected - value) < value * 0.05:
                                    position = {
                                        'symbol': ticker,
                                        'description': fund_pattern.title(),
                                        'shares': shares,
                                        'price': price,
                                        'value': value
                                    }
                                    if not any(p['symbol'] == ticker for p in positions):
                                        positions.append(position)
                        break

    # Fallback: text-based parsing
    if not positions:
        full_text = ""
        for page in pdf.pages:
            text = page.extract_text() or ""
            full_text += text + "\n"

        lines = full_text.split('\n')
        in_market_value = False

        for line in lines:
            line_lower = line.lower().strip()

            if 'your market value' in line_lower:
                in_market_value = True
                continue
            if in_market_value and ('fund performance' in line_lower or 'summary of expenses' in line_lower):
                break

            if not in_market_value:
                continue

            # Try to match fund patterns in this line
            for fund_pattern, ticker in FUND_PATTERNS:
                if fund_pattern in line_lower:
                    # Extract all numbers from this line
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    numbers = [clean_number(n) for n in numbers if clean_number(n) is not None]

                    if len(numbers) >= 3:
                        shares = numbers[0]
                        price = numbers[1]
                        value = numbers[2]

                        # Validate
                        if shares and price and value and shares * price > 0:
                            expected = shares * price
                            if abs(expected - value) < value * 0.1:
                                position = {
                                    'symbol': ticker,
                                    'description': fund_pattern.title(),
                                    'shares': shares,
                                    'price': price,
                                    'value': value
                                }
                                if not any(p['symbol'] == ticker for p in positions):
                                    positions.append(position)
                    break

    return positions


def parse_morgan_stanley_pdf(pdf):
    """Parse Morgan Stanley brokerage statement with all asset types."""
    positions = []

    # Get all text from the PDF
    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    lines = full_text.split('\n')

    # Track section state
    in_holdings_section = False
    current_section = None

    # Morgan Stanley section markers for different asset types
    section_start_markers = [
        'Security Description', 'MUTUAL FUNDS', 'EQUITIES', 'STOCKS',
        'EXCHANGE TRADED FUNDS', 'ETFs', 'FIXED INCOME', 'BONDS',
        'Account Detail', 'Your Holdings', 'Investment Summary',
        'COMMON STOCK', 'PREFERRED STOCK', 'CORPORATE BONDS'
    ]
    section_end_markers = [
        'TOTAL VALUE', 'ALLOCATION OF ASSETS', 'ACCOUNT ACTIVITY',
        'TRANSACTION HISTORY', 'Important Disclosures', 'Terms and Conditions'
    ]

    for i, line in enumerate(lines):
        line_stripped = line.strip()
        line_upper = line_stripped.upper()

        # Detect start of holdings section
        if any(marker.upper() in line_upper for marker in section_start_markers):
            in_holdings_section = True
            current_section = line_stripped
            continue

        # Detect end of holdings section
        if in_holdings_section and any(marker.upper() in line_upper for marker in section_end_markers):
            in_holdings_section = False
            current_section = None
            continue

        if not in_holdings_section:
            continue

        # Method 1: Look for ticker symbols in parentheses like "MSILF GOVERNMENT INST (MVRXX)"
        ticker_match = re.search(r'\(([A-Z]{2,5}X?)\)', line)
        if ticker_match:
            ticker = ticker_match.group(1)

            # Skip common false positives
            if ticker in EXCLUDED_WORDS:
                continue

            # Get the description (text before the parentheses)
            description = line.split('(')[0].strip()

            # Look for numbers on this line or nearby lines
            numbers = re.findall(r'[\d,]+\.[\d]+', line)

            # Check next few lines for numbers if not found on current line
            if len(numbers) < 2:
                for j in range(1, 4):
                    if i + j < len(lines):
                        more_nums = re.findall(r'[\d,]+\.[\d]+', lines[i + j])
                        numbers.extend(more_nums)
                    if len(numbers) >= 2:
                        break

            if len(numbers) >= 2:
                shares = clean_number(numbers[0])
                value = clean_number(numbers[-1])
                price = value / shares if shares > 0 else 1.0

                position = {
                    'symbol': ticker,
                    'description': description,
                    'shares': shares,
                    'price': round(price, 4),
                    'value': value
                }

                if not any(p['symbol'] == ticker for p in positions):
                    positions.append(position)
            continue

        # Method 2: Look for symbol at start of line (equities format)
        match = re.match(r'^([A-Z]{1,5})\s+', line)
        if match:
            symbol = match.group(1)
            if is_valid_symbol(symbol) and symbol not in EXCLUDED_WORDS:
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                if len(numbers) >= 2:
                    shares = clean_number(numbers[0])
                    value = clean_number(numbers[-1])

                    if shares and value and shares < 10000000:
                        # Extract description between symbol and first number
                        first_num = re.search(r'[\d,]+\.[\d]+', line)
                        desc_end = first_num.start() if first_num else len(line)
                        description = line[len(symbol):desc_end].strip()

                        position = {
                            'symbol': symbol,
                            'description': description[:50],
                            'shares': shares,
                            'price': round(value / shares, 4) if shares > 0 else None,
                            'value': value
                        }
                        if not any(p['symbol'] == symbol for p in positions):
                            positions.append(position)
                    continue

        # Method 3: Look for known symbols anywhere in line (within holdings section)
        for symbol in KNOWN_SYMBOLS:
            if re.search(rf'\b{symbol}\b', line):
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                if len(numbers) >= 2:
                    shares = clean_number(numbers[0])
                    value = clean_number(numbers[-1])

                    if shares and value and shares < 10000000:
                        position = {
                            'symbol': symbol,
                            'description': '',
                            'shares': shares,
                            'price': round(value / shares, 4) if shares > 0 else None,
                            'value': value
                        }
                        if not any(p['symbol'] == symbol for p in positions):
                            positions.append(position)
                break

    return positions


def detect_csv_format(content):
    """Detect the CSV format/brokerage based on content patterns."""
    content_lower = content.lower()

    # IBKR format detection
    if 'statement,' in content_lower or 'interactive brokers' in content_lower:
        return 'ibkr'
    if 'header,field name' in content_lower:
        return 'ibkr'

    # Fidelity format
    if 'account name/number' in content_lower and 'symbol' in content_lower:
        return 'fidelity'

    # Schwab format
    if 'schwab' in content_lower or 'positions_' in content_lower:
        return 'schwab'

    # Vanguard format
    if 'vanguard' in content_lower:
        return 'vanguard'

    # TD Ameritrade format
    if 'thinkorswim' in content_lower or 'td ameritrade' in content_lower:
        return 'tdameritrade'

    return 'generic'


def parse_ibkr_csv(content):
    """
    Parse Interactive Brokers multi-section CSV format.
    IBKR CSVs have multiple sections separated by blank lines,
    each section starting with a header row.
    """
    positions = []

    if isinstance(content, bytes):
        content = content.decode('utf-8-sig')

    lines = content.split('\n')
    current_section = None
    header_row = None
    header_indices = {}

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines
        if not line:
            current_section = None
            header_row = None
            header_indices = {}
            i += 1
            continue

        # Parse row
        try:
            reader = csv.reader(io.StringIO(line))
            row = next(reader)
        except:
            i += 1
            continue

        if not row:
            i += 1
            continue

        # Check if this is a section marker
        first_cell = row[0].strip().lower()

        # IBKR section markers
        if first_cell in ['statement', 'account information', 'positions', 'trades', 'open positions', 'mark-to-market', 'cash report']:
            current_section = first_cell
            i += 1
            continue

        # Check if this is a header row (contains common header words)
        row_lower = [cell.lower().strip() for cell in row]
        is_header = any(word in ' '.join(row_lower) for word in ['symbol', 'description', 'quantity', 'position', 'market value'])

        if is_header and (current_section in ['positions', 'open positions'] or 'position' in ' '.join(row_lower)):
            header_row = row
            header_indices = {}

            # Map columns using enhanced aliases
            for idx, col in enumerate(row):
                col_lower = col.lower().strip()
                if match_column(col, 'symbol'):
                    header_indices['symbol'] = idx
                elif match_column(col, 'description'):
                    header_indices['description'] = idx
                elif match_column(col, 'shares'):
                    header_indices['shares'] = idx
                elif match_column(col, 'price'):
                    header_indices['price'] = idx
                elif match_column(col, 'value'):
                    header_indices['value'] = idx
                elif match_column(col, 'cost_basis'):
                    header_indices['cost_basis'] = idx

            i += 1
            continue

        # Process data rows if we have a header
        if header_row and header_indices.get('symbol') is not None:
            symbol = None
            symbol_idx = header_indices.get('symbol')

            if symbol_idx is not None and symbol_idx < len(row):
                symbol_val = str(row[symbol_idx]).strip().upper()
                # Check if it's a CUSIP
                if len(symbol_val) == 9 and symbol_val.isalnum():
                    ticker = cusip_to_ticker(symbol_val)
                    symbol = ticker if ticker else symbol_val
                elif is_valid_symbol(symbol_val):
                    symbol = symbol_val

            if symbol:
                position = {'symbol': symbol}

                desc_idx = header_indices.get('description')
                if desc_idx is not None and desc_idx < len(row):
                    position['description'] = str(row[desc_idx]).strip()
                else:
                    position['description'] = symbol

                shares_idx = header_indices.get('shares')
                if shares_idx is not None and shares_idx < len(row):
                    position['shares'] = clean_number(row[shares_idx])

                price_idx = header_indices.get('price')
                if price_idx is not None and price_idx < len(row):
                    position['price'] = clean_number(row[price_idx])

                value_idx = header_indices.get('value')
                if value_idx is not None and value_idx < len(row):
                    position['value'] = clean_number(row[value_idx])

                cost_idx = header_indices.get('cost_basis')
                if cost_idx is not None and cost_idx < len(row):
                    position['cost_basis'] = clean_number(row[cost_idx])

                # Calculate value if not present
                if not position.get('value') and position.get('shares') and position.get('price'):
                    position['value'] = position['shares'] * position['price']

                if position.get('shares') or position.get('value'):
                    positions.append(position)

        i += 1

    return positions


def parse_csv_file(content):
    """Parse a CSV file from various brokerages using enhanced column detection."""
    positions = []

    if isinstance(content, bytes):
        content = content.decode('utf-8-sig')

    # Detect format
    csv_format = detect_csv_format(content)

    # Use specialized parser for IBKR
    if csv_format == 'ibkr':
        return parse_ibkr_csv(content)

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)

    if not rows:
        return positions

    # Find header row - look for rows containing position-related headers
    header_row = None
    header_index = 0

    for i, row in enumerate(rows):
        row_text = ' '.join([str(cell).lower() for cell in row])
        # Check for common header indicators
        if any(alias in row_text for alias in COLUMN_ALIASES['symbol']) or \
           any(alias in row_text for alias in COLUMN_ALIASES['shares']):
            header_row = row
            header_index = i
            break

    if not header_row:
        header_row = rows[0]
        header_index = 0

    # Map columns using enhanced aliases
    symbol_idx = None
    desc_idx = None
    shares_idx = None
    price_idx = None
    value_idx = None
    cost_idx = None

    for i, h in enumerate(header_row):
        h_lower = str(h).lower().strip()
        if match_column(h, 'symbol') and symbol_idx is None:
            symbol_idx = i
        elif match_column(h, 'description') and desc_idx is None:
            desc_idx = i
        elif match_column(h, 'shares') and shares_idx is None:
            shares_idx = i
        elif match_column(h, 'price') and price_idx is None:
            price_idx = i
        elif match_column(h, 'value') and value_idx is None:
            value_idx = i
        elif match_column(h, 'cost_basis') and cost_idx is None:
            cost_idx = i

    for row in rows[header_index + 1:]:
        if len(row) == 0:
            continue

        max_idx = max(filter(None, [symbol_idx, desc_idx, shares_idx, price_idx, value_idx, cost_idx]), default=0)
        if len(row) <= max_idx:
            continue

        symbol = None
        if symbol_idx is not None and symbol_idx < len(row):
            symbol_val = str(row[symbol_idx]).strip().upper()

            # Check if it looks like a CUSIP (9 alphanumeric characters)
            if len(symbol_val) == 9 and symbol_val.isalnum():
                ticker = cusip_to_ticker(symbol_val)
                symbol = ticker if ticker else symbol_val
            else:
                symbol = symbol_val

        # If no valid symbol found, scan the row
        if not symbol or not is_valid_symbol(symbol):
            for cell in row:
                cell_str = str(cell).strip()
                if is_valid_symbol(cell_str):
                    symbol = cell_str.upper()
                    break
                # Also check for CUSIP
                if len(cell_str) == 9 and cell_str.isalnum():
                    ticker = cusip_to_ticker(cell_str)
                    if ticker:
                        symbol = ticker
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

        if cost_idx is not None and cost_idx < len(row):
            position['cost_basis'] = clean_number(row[cost_idx])

        # Calculate value if missing but we have shares and price
        if not position.get('value') and position.get('shares') and position.get('price'):
            position['value'] = position['shares'] * position['price']

        if position['shares'] or position['value']:
            positions.append(position)

    return positions


def parse_robinhood_pdf(pdf):
    """Parse Robinhood brokerage/crypto statement."""
    positions = []

    full_text = ""
    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

    lines = full_text.split('\n')

    # Robinhood crypto format: "Bitcoin 0.03962234 BTC $3115.87 100%"
    # Or table format with headers: CRYPTOCURRENCY HELD IN ACCOUNT | QUANTITY | SYMBOL | MARKET VALUE

    in_crypto_section = False
    in_stocks_section = False

    # Crypto symbols mapping
    crypto_names = {
        'bitcoin': 'BTC', 'ethereum': 'ETH', 'solana': 'SOL', 'dogecoin': 'DOGE',
        'cardano': 'ADA', 'ripple': 'XRP', 'polkadot': 'DOT', 'avalanche': 'AVAX',
        'polygon': 'MATIC', 'chainlink': 'LINK', 'litecoin': 'LTC', 'uniswap': 'UNI',
        'aave': 'AAVE', 'shiba': 'SHIB', 'pepe': 'PEPE'
    }

    for i, line in enumerate(lines):
        line_stripped = line.strip()
        line_lower = line_stripped.lower()

        # Section detection
        if 'cryptocurrency' in line_lower or 'crypto' in line_lower or 'portfolio allocation' in line_lower:
            in_crypto_section = True
            in_stocks_section = False
            continue
        if 'stocks' in line_lower or 'equities' in line_lower or 'securities' in line_lower:
            in_stocks_section = True
            in_crypto_section = False
            continue
        if 'transaction' in line_lower or 'activity' in line_lower or 'disclosures' in line_lower:
            in_crypto_section = False
            in_stocks_section = False
            continue

        # Parse crypto positions
        if in_crypto_section or 'btc' in line_lower or 'eth' in line_lower:
            # Method 1: Match "CryptoName Quantity Symbol Value"
            # Example: "Bitcoin 0.03962234 BTC $3115.87 100%"
            for crypto_name, symbol in crypto_names.items():
                if crypto_name in line_lower:
                    # Extract numbers: quantity and value
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    # Also try to find dollar amounts
                    dollar_match = re.search(r'\$[\d,]+\.?\d*', line)

                    if numbers:
                        quantity = clean_number(numbers[0])
                        value = None

                        if dollar_match:
                            value = clean_number(dollar_match.group().replace('$', ''))
                        elif len(numbers) >= 2:
                            value = clean_number(numbers[-1])

                        if quantity and value:
                            price = value / quantity if quantity > 0 else value

                            position = {
                                'symbol': symbol,
                                'description': crypto_name.title(),
                                'shares': quantity,
                                'price': round(price, 2),
                                'value': value
                            }

                            if not any(p['symbol'] == symbol for p in positions):
                                positions.append(position)
                    break

            # Method 2: Direct symbol match (BTC, ETH, etc.)
            for symbol in ['BTC', 'ETH', 'SOL', 'DOGE', 'ADA', 'XRP', 'DOT', 'AVAX', 'MATIC', 'LINK', 'LTC']:
                if re.search(rf'\b{symbol}\b', line):
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    dollar_match = re.search(r'\$[\d,]+\.?\d*', line)

                    if numbers:
                        quantity = clean_number(numbers[0])
                        value = None

                        if dollar_match:
                            value = clean_number(dollar_match.group().replace('$', ''))
                        elif len(numbers) >= 2:
                            value = clean_number(numbers[-1])

                        if quantity and value and not any(p['symbol'] == symbol for p in positions):
                            price = value / quantity if quantity > 0 else value
                            positions.append({
                                'symbol': symbol,
                                'description': symbol,
                                'shares': quantity,
                                'price': round(price, 2),
                                'value': value
                            })
                    break

        # Parse stock positions (Robinhood also has stocks)
        if in_stocks_section:
            match = re.match(r'^([A-Z]{1,5})\s+', line)
            if match:
                symbol = match.group(1)
                if is_valid_symbol(symbol):
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    if len(numbers) >= 2:
                        shares = clean_number(numbers[0])
                        value = clean_number(numbers[-1])
                        if shares and value and shares < 10000000:
                            positions.append({
                                'symbol': symbol,
                                'description': '',
                                'shares': shares,
                                'price': round(value / shares, 2) if shares > 0 else None,
                                'value': value
                            })

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
        elif brokerage == 'stifel':
            positions = parse_stifel_pdf(pdf)
        elif brokerage == 'acropolis':
            positions = parse_acropolis_pdf(pdf)
        elif brokerage == 'morgan_stanley':
            positions = parse_morgan_stanley_pdf(pdf)
        elif brokerage == 'robinhood':
            positions = parse_robinhood_pdf(pdf)
        else:
            # Generic text-based parsing with section awareness
            full_text = ""
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"

            lines = full_text.split('\n')

            # Track whether we're in a holdings-like section
            in_holdings_section = False

            # Generic markers for holdings sections
            holdings_start = [
                'holdings', 'positions', 'investments', 'securities',
                'equities', 'stocks', 'funds', 'etf', 'bonds',
                'symbol', 'ticker', 'description', 'quantity', 'market value'
            ]
            holdings_end = [
                'transaction', 'activity', 'disclosures', 'terms',
                'important information', 'notices', 'footnotes'
            ]

            for line in lines:
                line_lower = line.lower().strip()

                # Check for section boundaries
                if any(marker in line_lower for marker in holdings_start):
                    in_holdings_section = True
                if any(marker in line_lower for marker in holdings_end):
                    in_holdings_section = False

                # Method 1: Symbol at start of line with numbers
                match = re.match(r'^([A-Z]{1,5})\s+', line)
                if match:
                    symbol = match.group(1)
                    if is_valid_symbol(symbol):
                        numbers = re.findall(r'[\d,]+\.[\d]+', line)
                        if len(numbers) >= 2:
                            shares = clean_number(numbers[0])
                            value = clean_number(numbers[-1])
                            # Validate reasonable values
                            if shares and value and shares < 10000000 and value > 0:
                                position = {
                                    'symbol': symbol,
                                    'description': '',
                                    'shares': shares,
                                    'price': round(value / shares, 2) if shares > 0 else None,
                                    'value': value
                                }
                                if not any(p['symbol'] == symbol for p in positions):
                                    positions.append(position)
                                continue

                # Method 2: Known symbols (only in holdings section to avoid false positives)
                if in_holdings_section:
                    for symbol in KNOWN_SYMBOLS:
                        # Word boundary match to avoid partial matches
                        if re.search(rf'\b{symbol}\b', line):
                            numbers = re.findall(r'[\d,]+\.[\d]+', line)
                            if len(numbers) >= 2:
                                shares = clean_number(numbers[0])
                                value = clean_number(numbers[-1])
                                if shares and value and shares < 10000000:
                                    position = {
                                        'symbol': symbol,
                                        'description': '',
                                        'shares': shares,
                                        'price': round(value / shares, 2) if shares > 0 else None,
                                        'value': value
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


def parse_image_file(content):
    """Parse an image file (PNG, JPG) using OCR."""
    global OCR_READER

    if not OCR_AVAILABLE:
        raise ValueError('OCR not available. Please install easyocr and Pillow.')

    positions = []

    # Lazy initialize OCR reader (it's slow to load)
    if OCR_READER is None:
        OCR_READER = easyocr.Reader(['en'], gpu=False)

    # Open image from bytes
    image = Image.open(io.BytesIO(content))

    # Convert to RGB if necessary (easyocr needs RGB)
    if image.mode != 'RGB':
        image = image.convert('RGB')

    # Save to bytes for easyocr
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='PNG')
    img_byte_arr.seek(0)

    # Extract text using OCR
    results = OCR_READER.readtext(img_byte_arr.getvalue())

    # Combine all text results
    full_text = '\n'.join([result[1] for result in results])

    # Detect brokerage from text
    brokerage = detect_brokerage_pdf(full_text)

    lines = full_text.split('\n')

    # Track section state
    in_holdings_section = False

    # Crypto symbols mapping for Robinhood-style statements
    crypto_names = {
        'bitcoin': 'BTC', 'ethereum': 'ETH', 'solana': 'SOL', 'dogecoin': 'DOGE',
        'cardano': 'ADA', 'ripple': 'XRP', 'polkadot': 'DOT', 'avalanche': 'AVAX',
        'polygon': 'MATIC', 'chainlink': 'LINK', 'litecoin': 'LTC', 'uniswap': 'UNI'
    }

    # Section markers
    holdings_start = [
        'holdings', 'positions', 'investments', 'securities', 'cryptocurrency',
        'portfolio allocation', 'equities', 'stocks', 'funds', 'etf', 'bonds',
        'symbol', 'quantity', 'market value'
    ]
    holdings_end = [
        'transaction', 'activity', 'disclosures', 'terms',
        'important information', 'this statement is provided'
    ]

    for line in lines:
        line_stripped = line.strip()
        line_lower = line_stripped.lower()

        # Check for section boundaries
        if any(marker in line_lower for marker in holdings_start):
            in_holdings_section = True
        if any(marker in line_lower for marker in holdings_end):
            in_holdings_section = False

        # Parse crypto positions (common in Robinhood screenshots)
        for crypto_name, symbol in crypto_names.items():
            if crypto_name in line_lower:
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                dollar_match = re.search(r'\$[\d,]+\.?\d*', line)

                if numbers:
                    quantity = clean_number(numbers[0])
                    value = None

                    if dollar_match:
                        value = clean_number(dollar_match.group().replace('$', ''))
                    elif len(numbers) >= 2:
                        value = clean_number(numbers[-1])

                    if quantity and value:
                        price = value / quantity if quantity > 0 else value
                        if not any(p['symbol'] == symbol for p in positions):
                            positions.append({
                                'symbol': symbol,
                                'description': crypto_name.title(),
                                'shares': quantity,
                                'price': round(price, 2),
                                'value': value
                            })
                break

        # Parse direct crypto symbols (BTC, ETH, etc.)
        for symbol in ['BTC', 'ETH', 'SOL', 'DOGE', 'ADA', 'XRP', 'DOT', 'AVAX', 'MATIC', 'LINK', 'LTC']:
            if re.search(rf'\b{symbol}\b', line):
                numbers = re.findall(r'[\d,]+\.[\d]+', line)
                dollar_match = re.search(r'\$[\d,]+\.?\d*', line)

                if numbers:
                    quantity = clean_number(numbers[0])
                    value = None

                    if dollar_match:
                        value = clean_number(dollar_match.group().replace('$', ''))
                    elif len(numbers) >= 2:
                        value = clean_number(numbers[-1])

                    if quantity and value and not any(p['symbol'] == symbol for p in positions):
                        price = value / quantity if quantity > 0 else value
                        positions.append({
                            'symbol': symbol,
                            'description': symbol,
                            'shares': quantity,
                            'price': round(price, 2),
                            'value': value
                        })
                break

        # Parse stock/ETF positions
        if in_holdings_section:
            match = re.match(r'^([A-Z]{1,5})\s+', line)
            if match:
                symbol = match.group(1)
                if is_valid_symbol(symbol):
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    if len(numbers) >= 2:
                        shares = clean_number(numbers[0])
                        value = clean_number(numbers[-1])
                        if shares and value and shares < 10000000:
                            if not any(p['symbol'] == symbol for p in positions):
                                positions.append({
                                    'symbol': symbol,
                                    'description': '',
                                    'shares': shares,
                                    'price': round(value / shares, 2) if shares > 0 else None,
                                    'value': value
                                })

            # Also check for known symbols mid-line
            for symbol in KNOWN_SYMBOLS:
                if re.search(rf'\b{symbol}\b', line):
                    numbers = re.findall(r'[\d,]+\.[\d]+', line)
                    if len(numbers) >= 2:
                        shares = clean_number(numbers[0])
                        value = clean_number(numbers[-1])
                        if shares and value and shares < 10000000:
                            if not any(p['symbol'] == symbol for p in positions):
                                positions.append({
                                    'symbol': symbol,
                                    'description': '',
                                    'shares': shares,
                                    'price': round(value / shares, 2) if shares > 0 else None,
                                    'value': value
                                })
                    break

    return positions, brokerage


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'})


@app.route('/market', methods=['GET'])
def get_market_data():
    """Get live market data for major indices and assets."""
    try:
        if not YFINANCE_AVAILABLE:
            return jsonify({'error': 'Market data unavailable'}), 503

        # Symbols to fetch
        symbols = {
            'spy': '^GSPC',      # S&P 500 index
            'qqq': '^IXIC',      # NASDAQ Composite
            'dia': '^DJI',       # Dow Jones
            'vix': '^VIX',       # VIX
            'tlt': '^TNX',       # 10-Year Treasury Yield
            'gld': 'GC=F',       # Gold Futures
            'btc': 'BTC-USD'     # Bitcoin
        }

        result = {}

        for key, symbol in symbols.items():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period='2d')

                if not hist.empty and len(hist) >= 1:
                    current = float(hist['Close'].iloc[-1])
                    previous = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current
                    change_pct = ((current - previous) / previous * 100) if previous > 0 else 0

                    result[key] = {
                        'value': round(current, 2),
                        'change': round(change_pct, 2)
                    }
                else:
                    result[key] = {'value': None, 'change': None}
            except Exception:
                result[key] = {'value': None, 'change': None}

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/search', methods=['GET'])
def search_stocks():
    """Search for stocks by name or symbol."""
    try:
        import requests

        query = request.args.get('q', '').strip()
        if not query or len(query) < 1:
            return jsonify({'results': []})

        # Use Yahoo Finance search API
        url = f"https://query1.finance.yahoo.com/v1/finance/search"
        params = {
            'q': query,
            'quotesCount': 8,
            'newsCount': 0,
            'enableFuzzyQuery': True,
            'quotesQueryId': 'tss_match_phrase_query'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, params=params, headers=headers, timeout=5)
        data = response.json()

        results = []
        for quote in data.get('quotes', []):
            # Filter to stocks and ETFs only
            quote_type = quote.get('quoteType', '')
            if quote_type in ['EQUITY', 'ETF']:
                results.append({
                    'symbol': quote.get('symbol', ''),
                    'name': quote.get('shortname') or quote.get('longname', ''),
                    'type': quote_type,
                    'exchange': quote.get('exchange', '')
                })

        return jsonify({'results': results})

    except Exception as e:
        return jsonify({'results': [], 'error': str(e)})


@app.route('/quote/<symbol>', methods=['GET'])
def get_quote(symbol):
    """Get current price quote for a symbol."""
    try:
        if not YFINANCE_AVAILABLE:
            return jsonify({'error': 'Quote service unavailable'}), 503

        symbol = symbol.upper().strip()
        ticker = yf.Ticker(symbol)

        # Get current price
        hist = ticker.history(period='5d')
        if hist.empty:
            return jsonify({'error': f'Symbol {symbol} not found'}), 404

        current_price = float(hist['Close'].iloc[-1])
        previous_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current_price
        change = current_price - previous_close
        change_percent = (change / previous_close * 100) if previous_close > 0 else 0

        # Get additional info
        info = ticker.info
        name = info.get('shortName') or info.get('longName') or symbol

        return jsonify({
            'symbol': symbol,
            'name': name,
            'price': round(current_price, 2),
            'change': round(change, 2),
            'changePercent': round(change_percent, 2),
            'currency': info.get('currency', 'USD')
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# AUTHENTICATION ENDPOINTS
# =============================================================================

@app.route('/auth/register', methods=['POST'])
def register():
    """Register a new user account."""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        name = data.get('name', '').strip()

        # Validation
        if not email or '@' not in email:
            return jsonify({'error': 'Valid email is required'}), 400

        if not password or len(password) < 8:
            return jsonify({'error': 'Password must be at least 8 characters'}), 400

        # Check if user already exists
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({'error': 'Email already registered'}), 409

        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        # Create user
        user = User(
            email=email,
            password_hash=password_hash,
            name=name or None
        )
        db.session.add(user)
        db.session.commit()

        # Generate JWT token
        access_token = create_access_token(identity=str(user.id))

        return jsonify({
            'message': 'Account created successfully',
            'user': user.to_dict(),
            'access_token': access_token
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Registration failed: {str(e)}'}), 500


@app.route('/auth/login', methods=['POST'])
def login():
    """Log in and get JWT token."""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        email = data.get('email', '').strip().lower()
        password = data.get('password', '')

        if not email or not password:
            return jsonify({'error': 'Email and password are required'}), 400

        # Find user
        user = User.query.filter_by(email=email).first()

        if not user:
            return jsonify({'error': 'Invalid email or password'}), 401

        # Verify password
        if not bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            return jsonify({'error': 'Invalid email or password'}), 401

        # Generate JWT token
        access_token = create_access_token(identity=str(user.id))

        return jsonify({
            'message': 'Login successful',
            'user': user.to_dict(),
            'access_token': access_token
        })

    except Exception as e:
        return jsonify({'error': f'Login failed: {str(e)}'}), 500


@app.route('/auth/me', methods=['GET'])
@jwt_required()
def get_current_user():
    """Get the current authenticated user."""
    try:
        user_id = int(get_jwt_identity())
        user = User.query.get(user_id)

        if not user:
            return jsonify({'error': 'User not found'}), 404

        return jsonify({'user': user.to_dict()})

    except Exception as e:
        return jsonify({'error': f'Failed to get user: {str(e)}'}), 500


@app.route('/auth/forgot-password', methods=['POST'])
def forgot_password():
    """Request a password reset email."""
    try:
        data = request.get_json()

        if not data or 'email' not in data:
            return jsonify({'error': 'Email is required'}), 400

        email = data['email'].strip().lower()

        # Find user by email
        user = User.query.filter_by(email=email).first()

        # Always return success to prevent email enumeration
        if not user:
            return jsonify({'message': 'If an account exists with that email, a reset link has been sent.'})

        # Generate reset token
        reset_token = secrets.token_urlsafe(32)
        user.reset_token = reset_token
        user.reset_token_expires = datetime.utcnow() + timedelta(hours=1)
        db.session.commit()

        # Send reset email
        frontend_url = os.environ.get('FRONTEND_URL', 'https://statement-scan.onrender.com')
        reset_link = f"{frontend_url}?reset_token={reset_token}"

        # Check if email is configured
        smtp_host = os.environ.get('SMTP_HOST')
        smtp_user = os.environ.get('SMTP_USER')
        smtp_pass = os.environ.get('SMTP_PASS')

        if smtp_host and smtp_user and smtp_pass:
            try:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = 'Reset Your Statement Scan Password'
                msg['From'] = smtp_user
                msg['To'] = email

                text_content = f"""
Hi,

You requested to reset your password for Statement Scan.

Click this link to reset your password (valid for 1 hour):
{reset_link}

If you didn't request this, you can safely ignore this email.

- Statement Scan Team
"""

                html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #0a2540; }}
        .container {{ max-width: 500px; margin: 0 auto; padding: 40px 20px; }}
        .logo {{ font-size: 24px; font-weight: 700; color: #635bff; margin-bottom: 32px; }}
        .btn {{ display: inline-block; background: #635bff; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; margin: 24px 0; }}
        .footer {{ margin-top: 40px; font-size: 14px; color: #666; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="logo">Statement Scan</div>
        <p>Hi,</p>
        <p>You requested to reset your password. Click the button below to create a new password:</p>
        <a href="{reset_link}" class="btn">Reset Password</a>
        <p>This link will expire in 1 hour.</p>
        <p>If you didn't request this password reset, you can safely ignore this email.</p>
        <div class="footer">
            <p>- The Statement Scan Team</p>
        </div>
    </div>
</body>
</html>
"""

                msg.attach(MIMEText(text_content, 'plain'))
                msg.attach(MIMEText(html_content, 'html'))

                smtp_port = int(os.environ.get('SMTP_PORT', 587))
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(smtp_user, email, msg.as_string())

            except Exception as email_error:
                # Log error but don't expose to user
                print(f"Email error: {email_error}")
                # Still return success to prevent enumeration
        else:
            # Email not configured - log the reset link for development
            print(f"Password reset link for {email}: {reset_link}")

        return jsonify({'message': 'If an account exists with that email, a reset link has been sent.'})

    except Exception as e:
        return jsonify({'error': f'Failed to process request: {str(e)}'}), 500


@app.route('/auth/reset-password', methods=['POST'])
def reset_password():
    """Reset password using a valid reset token."""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        token = data.get('token', '').strip()
        new_password = data.get('password', '')

        if not token:
            return jsonify({'error': 'Reset token is required'}), 400

        if not new_password or len(new_password) < 6:
            return jsonify({'error': 'Password must be at least 6 characters'}), 400

        # Find user by reset token
        user = User.query.filter_by(reset_token=token).first()

        if not user:
            return jsonify({'error': 'Invalid or expired reset link'}), 400

        # Check if token is expired
        if user.reset_token_expires and user.reset_token_expires < datetime.utcnow():
            user.reset_token = None
            user.reset_token_expires = None
            db.session.commit()
            return jsonify({'error': 'Reset link has expired. Please request a new one.'}), 400

        # Update password
        user.password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        user.reset_token = None
        user.reset_token_expires = None
        db.session.commit()

        return jsonify({'message': 'Password has been reset successfully. You can now log in.'})

    except Exception as e:
        return jsonify({'error': f'Failed to reset password: {str(e)}'}), 500


# =============================================================================
# PORTFOLIO ENDPOINTS
# =============================================================================

@app.route('/portfolios', methods=['GET'])
@jwt_required()
def list_portfolios():
    """List all portfolios for the current user."""
    try:
        user_id = int(get_jwt_identity())

        portfolios = Portfolio.query.filter_by(user_id=user_id)\
            .order_by(Portfolio.updated_at.desc())\
            .all()

        return jsonify({
            'portfolios': [p.to_dict() for p in portfolios],
            'count': len(portfolios)
        })

    except Exception as e:
        return jsonify({'error': f'Failed to list portfolios: {str(e)}'}), 500


@app.route('/portfolios', methods=['POST'])
@jwt_required()
def create_portfolio():
    """Save a new portfolio."""
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Portfolio name is required'}), 400

        positions = data.get('positions', [])
        if not positions:
            return jsonify({'error': 'Positions are required'}), 400

        # Calculate total value
        total_value = sum(p.get('value', 0) or 0 for p in positions)

        portfolio = Portfolio(
            user_id=user_id,
            name=name,
            description=data.get('description', '').strip() or None,
            positions=positions,
            total_value=total_value
        )
        db.session.add(portfolio)
        db.session.commit()

        return jsonify({
            'message': 'Portfolio saved successfully',
            'portfolio': portfolio.to_dict()
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to save portfolio: {str(e)}'}), 500


@app.route('/portfolios/<int:portfolio_id>', methods=['GET'])
@jwt_required()
def get_portfolio(portfolio_id):
    """Get a specific portfolio by ID."""
    try:
        user_id = int(get_jwt_identity())

        portfolio = Portfolio.query.filter_by(id=portfolio_id, user_id=user_id).first()

        if not portfolio:
            return jsonify({'error': 'Portfolio not found'}), 404

        return jsonify({'portfolio': portfolio.to_dict()})

    except Exception as e:
        return jsonify({'error': f'Failed to get portfolio: {str(e)}'}), 500


@app.route('/portfolios/<int:portfolio_id>', methods=['PUT'])
@jwt_required()
def update_portfolio(portfolio_id):
    """Update a portfolio."""
    try:
        user_id = int(get_jwt_identity())
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        portfolio = Portfolio.query.filter_by(id=portfolio_id, user_id=user_id).first()

        if not portfolio:
            return jsonify({'error': 'Portfolio not found'}), 404

        # Update fields
        if 'name' in data:
            name = data['name'].strip()
            if name:
                portfolio.name = name

        if 'description' in data:
            portfolio.description = data['description'].strip() or None

        if 'positions' in data:
            positions = data['positions']
            portfolio.positions = positions
            portfolio.total_value = sum(p.get('value', 0) or 0 for p in positions)

        db.session.commit()

        return jsonify({
            'message': 'Portfolio updated successfully',
            'portfolio': portfolio.to_dict()
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to update portfolio: {str(e)}'}), 500


@app.route('/portfolios/<int:portfolio_id>', methods=['DELETE'])
@jwt_required()
def delete_portfolio(portfolio_id):
    """Delete a portfolio."""
    try:
        user_id = int(get_jwt_identity())

        portfolio = Portfolio.query.filter_by(id=portfolio_id, user_id=user_id).first()

        if not portfolio:
            return jsonify({'error': 'Portfolio not found'}), 404

        db.session.delete(portfolio)
        db.session.commit()

        return jsonify({'message': 'Portfolio deleted successfully'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to delete portfolio: {str(e)}'}), 500


# =============================================================================
# PLAID INTEGRATION ENDPOINTS
# =============================================================================

@app.route('/plaid/status', methods=['GET'])
def plaid_status():
    """Check if Plaid integration is available."""
    return jsonify({
        'available': plaid_client.is_configured(),
        'env': plaid_client.env if plaid_client.is_configured() else None,
        'client_id_set': bool(plaid_client.client_id),
        'secret_set': bool(plaid_client.secret),
        'client_id_length': len(plaid_client.client_id) if plaid_client.client_id else 0,
        'secret_length': len(plaid_client.secret) if plaid_client.secret else 0
    })


@app.route('/plaid/create-link-token', methods=['POST'])
@jwt_required()
def create_link_token():
    """Create a Plaid Link token for the current user."""
    try:
        if not plaid_client.is_configured():
            return jsonify({'error': 'Plaid is not configured'}), 503

        user_id = int(get_jwt_identity())
        data = request.get_json(silent=True) or {}
        redirect_uri = data.get('redirect_uri')

        result = plaid_client.create_link_token(user_id, redirect_uri)

        return jsonify({
            'link_token': result.get('link_token'),
            'expiration': result.get('expiration')
        })

    except Exception as e:
        import traceback
        return jsonify({
            'error': f'Failed to create link token: {str(e)}',
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500


@app.route('/plaid/exchange-token', methods=['POST'])
@jwt_required()
def exchange_plaid_token():
    """Exchange a public token for an access token and save the connection."""
    try:
        if not plaid_client.is_configured():
            return jsonify({'error': 'Plaid is not configured'}), 503

        user_id = int(get_jwt_identity())
        data = request.get_json()

        if not data or 'public_token' not in data:
            return jsonify({'error': 'public_token is required'}), 400

        public_token = data['public_token']
        institution_name = data.get('institution_name', '')
        institution_id = data.get('institution_id', '')

        # Exchange token
        result = plaid_client.exchange_public_token(public_token)
        access_token = result['access_token']
        item_id = result['item_id']

        # Encrypt and save
        encrypted_token = plaid_client.encrypt_token(access_token)

        connection = PlaidConnection(
            user_id=user_id,
            item_id=item_id,
            access_token_encrypted=encrypted_token,
            institution_name=institution_name,
            institution_id=institution_id,
            last_synced=datetime.utcnow()
        )
        db.session.add(connection)
        db.session.commit()

        return jsonify({
            'message': 'Account connected successfully',
            'connection': connection.to_dict()
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to exchange token: {str(e)}'}), 500


@app.route('/plaid/connections', methods=['GET'])
@jwt_required()
def list_plaid_connections():
    """List all Plaid connections for the current user."""
    try:
        user_id = int(get_jwt_identity())

        connections = PlaidConnection.query.filter_by(user_id=user_id)\
            .order_by(PlaidConnection.created_at.desc())\
            .all()

        return jsonify({
            'connections': [c.to_dict() for c in connections],
            'count': len(connections)
        })

    except Exception as e:
        return jsonify({'error': f'Failed to list connections: {str(e)}'}), 500


@app.route('/plaid/connections/<int:connection_id>/sync', methods=['POST'])
@jwt_required()
def sync_plaid_connection(connection_id):
    """Sync holdings from a Plaid connection."""
    try:
        if not plaid_client.is_configured():
            return jsonify({'error': 'Plaid is not configured'}), 503

        user_id = int(get_jwt_identity())

        connection = PlaidConnection.query.filter_by(
            id=connection_id, user_id=user_id
        ).first()

        if not connection:
            return jsonify({'error': 'Connection not found'}), 404

        # Decrypt access token
        access_token = plaid_client.decrypt_token(connection.access_token_encrypted)

        # Get holdings
        holdings_response = plaid_client.get_holdings(access_token)

        # Convert to positions
        positions = plaid_client.holdings_to_positions(holdings_response)

        # Update last synced
        connection.last_synced = datetime.utcnow()
        db.session.commit()

        return jsonify({
            'positions': positions,
            'count': len(positions),
            'accounts': holdings_response.get('accounts', []),
            'connection': connection.to_dict()
        })

    except Exception as e:
        return jsonify({'error': f'Failed to sync holdings: {str(e)}'}), 500


@app.route('/plaid/connections/<int:connection_id>', methods=['DELETE'])
@jwt_required()
def delete_plaid_connection(connection_id):
    """Disconnect a Plaid account."""
    try:
        user_id = int(get_jwt_identity())

        connection = PlaidConnection.query.filter_by(
            id=connection_id, user_id=user_id
        ).first()

        if not connection:
            return jsonify({'error': 'Connection not found'}), 404

        # Try to remove item from Plaid
        if plaid_client.is_configured():
            try:
                access_token = plaid_client.decrypt_token(connection.access_token_encrypted)
                plaid_client.remove_item(access_token)
            except Exception:
                pass  # Continue even if Plaid removal fails

        # Delete from database
        db.session.delete(connection)
        db.session.commit()

        return jsonify({'message': 'Connection removed successfully'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to remove connection: {str(e)}'}), 500


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
            # Try to detect brokerage from CSV content
            csv_format = detect_csv_format(content if isinstance(content, str) else content.decode('utf-8-sig', errors='ignore'))
            brokerage = csv_format if csv_format != 'generic' else 'csv'
        elif filename.endswith(('.ofx', '.qfx')):
            # OFX/QFX format (Open Financial Exchange / Quicken)
            positions, brokerage = parse_ofx_file(content)
        elif filename.endswith('.pdf'):
            positions, brokerage = parse_pdf_file(content)
        elif filename.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            if not OCR_AVAILABLE:
                return jsonify({'error': 'Image parsing not available. Please upload a PDF or CSV file.'}), 400
            positions, brokerage = parse_image_file(content)
        else:
            return jsonify({'error': 'Unsupported file type. Please upload a PDF, CSV, OFX, QFX, or image file (PNG, JPG).'}), 400

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

    # Pattern-based detection for money market funds (typically end in XX)
    if len(symbol) == 5 and symbol.endswith('XX'):
        return {
            'asset_class': 'Cash',
            'sub_class': 'Money Market',
            'sector': 'Money Market',
            'geography': 'US'
        }

    # Pattern-based detection for bond funds (often end in X and contain bond-related letters)
    if len(symbol) >= 4 and symbol.endswith('X') and any(c in symbol for c in ['B', 'T', 'G']):
        # Could be a bond fund - check for common bond fund patterns
        if 'BND' in symbol or 'TRS' in symbol or 'GOV' in symbol:
            return {
                'asset_class': 'Bonds',
                'sub_class': 'US Aggregate',
                'sector': 'Bonds',
                'geography': 'US'
            }

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

        # Handle single symbol case - ensure data is a DataFrame
        if isinstance(data, pd.Series):
            data = data.to_frame()
            data.columns = symbols + ['SPY'] if 'SPY' in symbols else [symbols[0]]

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
    """Calculate historical returns and performance chart data for up to 5 years with multiple benchmarks."""
    if not YFINANCE_AVAILABLE:
        return {
            'returns': {},
            'chart_data': None,
            'benchmarks': {},
            'error': 'yfinance not available'
        }

    total_value = sum(p.get('value', 0) or 0 for p in positions)
    if total_value == 0:
        return {'returns': {}, 'chart_data': None, 'benchmarks': {}}

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
        return {'returns': {}, 'chart_data': None, 'benchmarks': {}}

    try:
        # Get 5+ years of historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1900)  # ~5.2 years

        symbols = list(weights.keys())

        # Benchmarks: S&P 500, Total Bond, Total World, 60/40 will be calculated
        benchmark_symbols = ['SPY', 'AGG', 'VT']

        # Download price data for portfolio and benchmarks
        all_symbols = list(set(symbols + benchmark_symbols))
        data = yf.download(
            all_symbols,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True
        )['Close']

        if data.empty:
            return {'returns': {}, 'chart_data': None, 'benchmarks': {}, 'error': 'No price data'}

        # Handle single symbol case
        if isinstance(data, pd.Series):
            data = data.to_frame()
            data.columns = [all_symbols[0]]

        # Forward fill missing data
        data = data.ffill().bfill()

        # Calculate daily returns
        returns = data.pct_change().iloc[1:]

        if returns.empty:
            return {'returns': {}, 'chart_data': None, 'benchmarks': {}}

        # Calculate portfolio returns (weighted)
        portfolio_returns = pd.Series(0.0, index=returns.index)
        for symbol, weight in weights.items():
            if symbol in returns.columns:
                symbol_returns = returns[symbol].fillna(0)
                portfolio_returns += symbol_returns * weight
        # Cash portion earns ~5% annual
        if cash_weight > 0:
            daily_cash_return = (1.05 ** (1/252)) - 1
            portfolio_returns += cash_weight * daily_cash_return

        # Calculate benchmark returns
        benchmark_returns = {}
        benchmark_cumulative = {}

        # S&P 500
        if 'SPY' in returns.columns:
            benchmark_returns['sp500'] = returns['SPY']
            benchmark_cumulative['sp500'] = (1 + returns['SPY']).cumprod()

        # Total Bond Market
        if 'AGG' in returns.columns:
            benchmark_returns['bonds'] = returns['AGG']
            benchmark_cumulative['bonds'] = (1 + returns['AGG']).cumprod()

        # Total World
        if 'VT' in returns.columns:
            benchmark_returns['world'] = returns['VT']
            benchmark_cumulative['world'] = (1 + returns['VT']).cumprod()

        # 60/40 Portfolio (60% SPY, 40% AGG)
        if 'SPY' in returns.columns and 'AGG' in returns.columns:
            sixty_forty = returns['SPY'] * 0.6 + returns['AGG'] * 0.4
            benchmark_returns['sixty_forty'] = sixty_forty
            benchmark_cumulative['sixty_forty'] = (1 + sixty_forty).cumprod()

        # Calculate cumulative returns for chart
        portfolio_cumulative = (1 + portfolio_returns).cumprod()

        # Calculate period returns
        today = returns.index[-1]
        period_returns = {}

        def calc_return(cumulative, days_back):
            target_date = today - timedelta(days=days_back)
            valid_dates = cumulative.index[cumulative.index <= target_date]
            if len(valid_dates) == 0:
                return None
            start_idx = valid_dates[-1]
            ret = (cumulative.iloc[-1] / cumulative.loc[start_idx] - 1) * 100
            return round(float(ret), 2)

        periods = [('1M', 30), ('3M', 90), ('6M', 180), ('1Y', 365), ('3Y', 1095), ('5Y', 1825)]

        for period_name, days in periods:
            port_ret = calc_return(portfolio_cumulative, days)
            period_returns[period_name] = {
                'portfolio': port_ret,
                'sp500': calc_return(benchmark_cumulative.get('sp500', pd.Series()), days) if 'sp500' in benchmark_cumulative else None,
                'bonds': calc_return(benchmark_cumulative.get('bonds', pd.Series()), days) if 'bonds' in benchmark_cumulative else None,
                'world': calc_return(benchmark_cumulative.get('world', pd.Series()), days) if 'world' in benchmark_cumulative else None,
                'sixty_forty': calc_return(benchmark_cumulative.get('sixty_forty', pd.Series()), days) if 'sixty_forty' in benchmark_cumulative else None,
                # Keep 'benchmark' for backwards compatibility
                'benchmark': calc_return(benchmark_cumulative.get('sp500', pd.Series()), days) if 'sp500' in benchmark_cumulative else None
            }

        # YTD
        year_start = datetime(today.year, 1, 1)
        valid_dates = portfolio_cumulative.index[portfolio_cumulative.index >= year_start]
        if len(valid_dates) > 0:
            start_idx = valid_dates[0]
            port_ytd = (portfolio_cumulative.iloc[-1] / portfolio_cumulative.loc[start_idx] - 1) * 100

            def calc_ytd(cumulative):
                if start_idx in cumulative.index:
                    return round(float((cumulative.iloc[-1] / cumulative.loc[start_idx] - 1) * 100), 2)
                return None

            period_returns['YTD'] = {
                'portfolio': round(float(port_ytd), 2),
                'sp500': calc_ytd(benchmark_cumulative.get('sp500', pd.Series())) if 'sp500' in benchmark_cumulative else None,
                'bonds': calc_ytd(benchmark_cumulative.get('bonds', pd.Series())) if 'bonds' in benchmark_cumulative else None,
                'world': calc_ytd(benchmark_cumulative.get('world', pd.Series())) if 'world' in benchmark_cumulative else None,
                'sixty_forty': calc_ytd(benchmark_cumulative.get('sixty_forty', pd.Series())) if 'sixty_forty' in benchmark_cumulative else None,
                'benchmark': calc_ytd(benchmark_cumulative.get('sp500', pd.Series())) if 'sp500' in benchmark_cumulative else None
            }
        else:
            period_returns['YTD'] = {'portfolio': None, 'sp500': None, 'bonds': None, 'world': None, 'sixty_forty': None, 'benchmark': None}

        # Generate chart data (weekly)
        portfolio_weekly = portfolio_cumulative.resample('W').last().dropna()

        chart_data = None
        if len(portfolio_weekly) > 0:
            portfolio_normalized = (portfolio_weekly / portfolio_weekly.iloc[0]) * 100
            chart_data = {
                'labels': [d.strftime('%Y-%m-%d') for d in portfolio_normalized.index],
                'portfolio': [round(float(v), 2) for v in portfolio_normalized.values],
            }

            # Add all benchmarks to chart
            for bm_name, bm_cum in benchmark_cumulative.items():
                bm_weekly = bm_cum.resample('W').last().dropna()
                if len(bm_weekly) > 0:
                    # Align to portfolio dates
                    bm_aligned = bm_weekly.reindex(portfolio_weekly.index, method='ffill')
                    if len(bm_aligned.dropna()) > 0:
                        bm_normalized = (bm_aligned / bm_aligned.iloc[0]) * 100
                        chart_data[bm_name] = [round(float(v), 2) if pd.notna(v) else None for v in bm_normalized.values]

            # Keep 'benchmark' key for backwards compatibility (S&P 500)
            if 'sp500' in chart_data:
                chart_data['benchmark'] = chart_data['sp500']

        return {
            'returns': period_returns,
            'chart_data': chart_data,
            'benchmarks': {
                'sp500': {'name': 'S&P 500', 'symbol': 'SPY'},
                'bonds': {'name': 'US Bonds', 'symbol': 'AGG'},
                'world': {'name': 'Total World', 'symbol': 'VT'},
                'sixty_forty': {'name': '60/40 Portfolio', 'symbol': 'SPY/AGG'}
            }
        }

    except Exception as e:
        return {
            'returns': {},
            'chart_data': None,
            'benchmarks': {},
            'error': str(e)
        }


def calculate_projections(positions, allocations):
    """Calculate portfolio projections using Capital Market Assumptions and Monte Carlo simulation."""
    if not YFINANCE_AVAILABLE or np is None:
        return {
            'capital_market_assumptions': {},
            'monte_carlo': None,
            'error': 'numpy/yfinance not available'
        }

    total_value = sum(p.get('value', 0) or 0 for p in positions)
    if total_value == 0:
        return {'capital_market_assumptions': {}, 'monte_carlo': None}

    # Capital Market Assumptions (10-year forward estimates)
    # Based on typical institutional assumptions
    CMA = {
        'Stocks': {'expected_return': 0.07, 'volatility': 0.16},  # 7% return, 16% vol
        'Bonds': {'expected_return': 0.04, 'volatility': 0.05},   # 4% return, 5% vol
        'Cash': {'expected_return': 0.03, 'volatility': 0.01},    # 3% return, 1% vol
        'Real Estate': {'expected_return': 0.06, 'volatility': 0.14},  # 6% return, 14% vol
        'Crypto': {'expected_return': 0.10, 'volatility': 0.60},  # 10% return, 60% vol
        'Commodities': {'expected_return': 0.04, 'volatility': 0.18}  # 4% return, 18% vol
    }

    # Get asset allocation
    asset_alloc = allocations.get('asset_allocation', {})

    # Calculate portfolio expected return and volatility
    portfolio_return = 0
    portfolio_vol_squared = 0

    for asset_class, pct in asset_alloc.items():
        weight = pct / 100
        if asset_class in CMA:
            portfolio_return += weight * CMA[asset_class]['expected_return']
            # Simplified: assume no correlation for vol (conservative)
            portfolio_vol_squared += (weight * CMA[asset_class]['volatility']) ** 2

    portfolio_volatility = np.sqrt(portfolio_vol_squared)

    # Monte Carlo Simulation
    num_simulations = 1000
    years = 10
    months = years * 12

    # Monthly parameters
    monthly_return = portfolio_return / 12
    monthly_vol = portfolio_volatility / np.sqrt(12)

    # Run simulations
    np.random.seed(42)  # For reproducibility
    simulations = np.zeros((num_simulations, months + 1))
    simulations[:, 0] = total_value

    for i in range(num_simulations):
        for m in range(1, months + 1):
            random_return = np.random.normal(monthly_return, monthly_vol)
            simulations[i, m] = simulations[i, m-1] * (1 + random_return)

    # Calculate percentiles at each time point
    percentiles = [5, 25, 50, 75, 95]
    projection_data = {
        'labels': [f'Year {y}' for y in range(years + 1)],
        'percentiles': {}
    }

    # Sample yearly (every 12 months)
    yearly_indices = [0] + [12 * y for y in range(1, years + 1)]

    for p in percentiles:
        values = [round(float(np.percentile(simulations[:, idx], p)), 0) for idx in yearly_indices]
        projection_data['percentiles'][f'p{p}'] = values

    # Calculate expected value path (using CMA)
    expected_path = [total_value]
    for y in range(1, years + 1):
        expected_path.append(round(total_value * ((1 + portfolio_return) ** y), 0))
    projection_data['expected'] = expected_path

    # Summary statistics at 10 years
    final_values = simulations[:, -1]
    monte_carlo_summary = {
        'median': round(float(np.median(final_values)), 0),
        'mean': round(float(np.mean(final_values)), 0),
        'p5': round(float(np.percentile(final_values, 5)), 0),
        'p25': round(float(np.percentile(final_values, 25)), 0),
        'p75': round(float(np.percentile(final_values, 75)), 0),
        'p95': round(float(np.percentile(final_values, 95)), 0),
        'min': round(float(np.min(final_values)), 0),
        'max': round(float(np.max(final_values)), 0),
        'prob_gain': round(float(np.sum(final_values > total_value) / num_simulations * 100), 1),
        'prob_double': round(float(np.sum(final_values > total_value * 2) / num_simulations * 100), 1)
    }

    return {
        'capital_market_assumptions': {
            'expected_annual_return': round(portfolio_return * 100, 2),
            'expected_volatility': round(portfolio_volatility * 100, 2),
            'assumptions': {k: {'return': v['expected_return'] * 100, 'volatility': v['volatility'] * 100} for k, v in CMA.items()}
        },
        'monte_carlo': {
            'simulations': num_simulations,
            'years': years,
            'starting_value': total_value,
            'projection_data': projection_data,
            'summary': monte_carlo_summary
        }
    }


def calculate_scenario_analysis(positions, allocations):
    """Calculate portfolio impact under various stress scenarios."""
    total_value = sum(p.get('value', 0) or 0 for p in positions)
    if total_value == 0:
        return {'scenarios': []}

    # Get asset allocation percentages
    asset_alloc = allocations.get('asset_allocation', {})
    stocks_pct = asset_alloc.get('Stocks', 0) / 100
    bonds_pct = asset_alloc.get('Bonds', 0) / 100
    cash_pct = asset_alloc.get('Cash', 0) / 100
    real_estate_pct = asset_alloc.get('Real Estate', 0) / 100
    crypto_pct = asset_alloc.get('Crypto', 0) / 100
    commodities_pct = asset_alloc.get('Commodities', 0) / 100

    # Define scenarios with asset class impacts (as decimals)
    scenarios = [
        {
            'name': '2008 Financial Crisis',
            'description': 'Severe market downturn similar to 2008',
            'impacts': {
                'Stocks': -0.50,
                'Bonds': 0.05,  # Flight to safety
                'Cash': 0.02,
                'Real Estate': -0.35,
                'Crypto': -0.70,
                'Commodities': -0.30
            }
        },
        {
            'name': 'COVID-19 Crash',
            'description': 'Rapid market selloff like March 2020',
            'impacts': {
                'Stocks': -0.34,
                'Bonds': 0.03,
                'Cash': 0.01,
                'Real Estate': -0.25,
                'Crypto': -0.50,
                'Commodities': -0.25
            }
        },
        {
            'name': 'Dot-Com Bubble',
            'description': 'Tech-focused bear market',
            'impacts': {
                'Stocks': -0.45,
                'Bonds': 0.08,
                'Cash': 0.03,
                'Real Estate': -0.05,
                'Crypto': -0.80,
                'Commodities': -0.10
            }
        },
        {
            'name': 'Rising Interest Rates',
            'description': 'Sharp rate hikes impacting bond prices',
            'impacts': {
                'Stocks': -0.15,
                'Bonds': -0.20,
                'Cash': 0.04,
                'Real Estate': -0.20,
                'Crypto': -0.25,
                'Commodities': 0.05
            }
        },
        {
            'name': 'High Inflation',
            'description': 'Sustained inflation above 8%',
            'impacts': {
                'Stocks': -0.10,
                'Bonds': -0.15,
                'Cash': -0.05,  # Purchasing power loss
                'Real Estate': 0.05,  # Inflation hedge
                'Crypto': -0.20,
                'Commodities': 0.15  # Inflation hedge
            }
        },
        {
            'name': 'Mild Recession',
            'description': 'Moderate economic contraction',
            'impacts': {
                'Stocks': -0.20,
                'Bonds': 0.05,
                'Cash': 0.02,
                'Real Estate': -0.15,
                'Crypto': -0.35,
                'Commodities': -0.15
            }
        },
        {
            'name': 'Bull Market Rally',
            'description': 'Strong market expansion (+25%)',
            'impacts': {
                'Stocks': 0.25,
                'Bonds': -0.03,
                'Cash': 0.02,
                'Real Estate': 0.15,
                'Crypto': 0.50,
                'Commodities': 0.10
            }
        }
    ]

    results = []
    for scenario in scenarios:
        impacts = scenario['impacts']

        # Calculate weighted portfolio impact
        portfolio_impact = (
            stocks_pct * impacts.get('Stocks', 0) +
            bonds_pct * impacts.get('Bonds', 0) +
            cash_pct * impacts.get('Cash', 0) +
            real_estate_pct * impacts.get('Real Estate', 0) +
            crypto_pct * impacts.get('Crypto', 0) +
            commodities_pct * impacts.get('Commodities', 0)
        )

        projected_value = total_value * (1 + portfolio_impact)
        value_change = projected_value - total_value

        results.append({
            'name': scenario['name'],
            'description': scenario['description'],
            'portfolio_impact': round(portfolio_impact * 100, 2),
            'projected_value': round(projected_value, 2),
            'value_change': round(value_change, 2)
        })

    # Sort by impact (worst first, but positive last)
    results.sort(key=lambda x: x['portfolio_impact'])

    return {'scenarios': results}


@app.route('/compare', methods=['POST'])
@optional_jwt_required()
def compare_portfolios():
    """Compare two portfolios side-by-side."""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        portfolio_a_input = data.get('portfolio_a')
        portfolio_b_input = data.get('portfolio_b')

        if not portfolio_a_input or not portfolio_b_input:
            return jsonify({'error': 'Both portfolio_a and portfolio_b are required'}), 400

        # Helper to get positions from portfolio input
        def get_positions(portfolio_input):
            if 'positions' in portfolio_input:
                return portfolio_input['positions']
            elif 'id' in portfolio_input:
                # Load from database
                user_id = int(get_jwt_identity())
                if not user_id:
                    raise ValueError('Authentication required to load saved portfolios')
                portfolio = Portfolio.query.filter_by(id=portfolio_input['id'], user_id=user_id).first()
                if not portfolio:
                    raise ValueError(f'Portfolio {portfolio_input["id"]} not found')
                return portfolio.positions
            else:
                raise ValueError('Portfolio must have either "positions" or "id"')

        # Get positions for both portfolios
        try:
            positions_a = get_positions(portfolio_a_input)
            positions_b = get_positions(portfolio_b_input)
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        # Analyze both portfolios
        def analyze(positions):
            allocations = calculate_allocations(positions)
            concentration = calculate_concentration(positions)
            risk_metrics = calculate_risk_metrics(positions)
            historical_performance = calculate_historical_performance(positions)
            scenario_analysis = calculate_scenario_analysis(positions, allocations)

            # Add classification to each position
            classified_positions = []
            for pos in positions:
                classified_pos = pos.copy()
                classification = get_classification(pos.get('symbol', ''))
                classified_pos['classification'] = classification
                classified_positions.append(classified_pos)

            return {
                'positions': classified_positions,
                'total_value': allocations['total_value'],
                'asset_allocation': allocations['asset_allocation'],
                'asset_allocation_values': allocations['asset_allocation_values'],
                'sub_class_allocation': allocations['sub_class_allocation'],
                'sector_exposure': allocations['sector_exposure'],
                'geography': allocations['geography'],
                'concentration': concentration,
                'risk_metrics': risk_metrics,
                'historical_performance': historical_performance,
                'scenario_analysis': scenario_analysis
            }

        analysis_a = analyze(positions_a)
        analysis_b = analyze(positions_b)

        # Calculate differences
        def calc_diff(dict_a, dict_b):
            all_keys = set(dict_a.keys()) | set(dict_b.keys())
            return {k: round((dict_b.get(k, 0) or 0) - (dict_a.get(k, 0) or 0), 2) for k in all_keys}

        def calc_metric_diff(metrics_a, metrics_b, key):
            val_a = metrics_a.get(key)
            val_b = metrics_b.get(key)
            if val_a is not None and val_b is not None:
                return round(val_b - val_a, 2)
            return None

        # Risk metric differences
        risk_diff = {
            'volatility': calc_metric_diff(analysis_a['risk_metrics'], analysis_b['risk_metrics'], 'volatility'),
            'beta': calc_metric_diff(analysis_a['risk_metrics'], analysis_b['risk_metrics'], 'beta'),
            'sharpe_ratio': calc_metric_diff(analysis_a['risk_metrics'], analysis_b['risk_metrics'], 'sharpe_ratio'),
            'max_drawdown': calc_metric_diff(analysis_a['risk_metrics'], analysis_b['risk_metrics'], 'max_drawdown')
        }

        # Performance differences
        perf_a = analysis_a.get('historical_performance', {}).get('returns', {})
        perf_b = analysis_b.get('historical_performance', {}).get('returns', {})
        performance_diff = {}
        for period in ['1M', '3M', '6M', 'YTD', '1Y', '3Y', '5Y']:
            val_a = perf_a.get(period, {}).get('portfolio')
            val_b = perf_b.get(period, {}).get('portfolio')
            if val_a is not None and val_b is not None:
                performance_diff[period] = round(val_b - val_a, 2)
            else:
                performance_diff[period] = None

        comparison = {
            'allocation_diff': calc_diff(analysis_a['asset_allocation'], analysis_b['asset_allocation']),
            'sector_diff': calc_diff(analysis_a['sector_exposure'], analysis_b['sector_exposure']),
            'geography_diff': calc_diff(analysis_a['geography'], analysis_b['geography']),
            'risk_diff': risk_diff,
            'performance_diff': performance_diff,
            'total_value_diff': round((analysis_b['total_value'] or 0) - (analysis_a['total_value'] or 0), 2)
        }

        return jsonify({
            'portfolio_a': analysis_a,
            'portfolio_b': analysis_b,
            'comparison': comparison
        })

    except Exception as e:
        return jsonify({'error': f'Comparison failed: {str(e)}'}), 500


@app.route('/what-if', methods=['POST'])
@optional_jwt_required()
def what_if_analysis():
    """Perform what-if analysis on a portfolio with hypothetical changes."""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Get base portfolio positions
        base_positions = None
        if 'positions' in data:
            base_positions = data['positions']
        elif 'base_portfolio_id' in data:
            user_id = int(get_jwt_identity())
            if not user_id:
                return jsonify({'error': 'Authentication required to load saved portfolios'}), 401
            portfolio = Portfolio.query.filter_by(id=data['base_portfolio_id'], user_id=user_id).first()
            if not portfolio:
                return jsonify({'error': 'Portfolio not found'}), 404
            base_positions = portfolio.positions

        if not base_positions:
            return jsonify({'error': 'Base portfolio positions are required'}), 400

        changes = data.get('changes', [])

        # Create a deep copy of positions for modification
        import copy
        modified_positions = copy.deepcopy(base_positions)

        # Track execution costs
        total_cost = 0.0

        # Apply changes
        for change in changes:
            action = change.get('action')
            symbol = change.get('symbol', '').upper()

            if action == 'add':
                # Add new position
                shares = change.get('shares', 0)
                price = change.get('price', 0)
                value = shares * price
                total_cost += value

                # Check if position already exists
                existing = next((p for p in modified_positions if p.get('symbol', '').upper() == symbol), None)
                if existing:
                    # Add to existing position
                    existing['shares'] = (existing.get('shares') or 0) + shares
                    existing['value'] = (existing.get('value') or 0) + value
                    if existing['shares'] > 0:
                        existing['price'] = existing['value'] / existing['shares']
                else:
                    # Create new position
                    modified_positions.append({
                        'symbol': symbol,
                        'description': change.get('description', ''),
                        'shares': shares,
                        'price': price,
                        'value': value
                    })

            elif action == 'remove':
                # Remove position entirely
                pos_to_remove = next((p for p in modified_positions if p.get('symbol', '').upper() == symbol), None)
                if pos_to_remove:
                    total_cost += pos_to_remove.get('value', 0)  # Selling generates cash
                    modified_positions = [p for p in modified_positions if p.get('symbol', '').upper() != symbol]

            elif action == 'adjust':
                # Adjust shares in existing position
                new_shares = change.get('new_shares', 0)
                pos = next((p for p in modified_positions if p.get('symbol', '').upper() == symbol), None)
                if pos:
                    old_shares = pos.get('shares') or 0
                    price = pos.get('price') or (pos.get('value', 0) / old_shares if old_shares > 0 else 0)
                    share_diff = new_shares - old_shares
                    total_cost += abs(share_diff * price)

                    pos['shares'] = new_shares
                    pos['value'] = new_shares * price
                    pos['price'] = price

            elif action == 'rebalance':
                # Rebalance to target allocation
                target = change.get('target', {})  # e.g., {'Stocks': 60, 'Bonds': 40}

                total_value = sum(p.get('value', 0) or 0 for p in modified_positions)
                if total_value == 0:
                    continue

                # Calculate current allocation by asset class
                current_by_class = {}
                for pos in modified_positions:
                    cls = get_classification(pos.get('symbol', ''))
                    asset_class = cls['asset_class']
                    current_by_class[asset_class] = current_by_class.get(asset_class, 0) + (pos.get('value', 0) or 0)

                # Adjust each position proportionally to reach target
                for pos in modified_positions:
                    cls = get_classification(pos.get('symbol', ''))
                    asset_class = cls['asset_class']

                    if asset_class not in target:
                        continue

                    current_class_value = current_by_class.get(asset_class, 0)
                    target_class_value = total_value * (target[asset_class] / 100)

                    if current_class_value > 0:
                        # Scale position proportionally
                        pos_pct_of_class = (pos.get('value', 0) or 0) / current_class_value
                        new_value = target_class_value * pos_pct_of_class
                        old_value = pos.get('value', 0) or 0

                        total_cost += abs(new_value - old_value)

                        pos['value'] = new_value
                        if pos.get('price') and pos['price'] > 0:
                            pos['shares'] = new_value / pos['price']

        # Analyze both original and modified portfolios
        def analyze(positions):
            allocations = calculate_allocations(positions)
            concentration = calculate_concentration(positions)
            risk_metrics = calculate_risk_metrics(positions)

            classified_positions = []
            for pos in positions:
                classified_pos = pos.copy()
                classification = get_classification(pos.get('symbol', ''))
                classified_pos['classification'] = classification
                classified_positions.append(classified_pos)

            return {
                'positions': classified_positions,
                'total_value': allocations['total_value'],
                'asset_allocation': allocations['asset_allocation'],
                'asset_allocation_values': allocations['asset_allocation_values'],
                'sub_class_allocation': allocations['sub_class_allocation'],
                'sector_exposure': allocations['sector_exposure'],
                'geography': allocations['geography'],
                'concentration': concentration,
                'risk_metrics': risk_metrics
            }

        original_analysis = analyze(base_positions)
        modified_analysis = analyze(modified_positions)

        # Calculate impact
        def calc_diff(dict_a, dict_b):
            all_keys = set(dict_a.keys()) | set(dict_b.keys())
            return {k: round((dict_b.get(k, 0) or 0) - (dict_a.get(k, 0) or 0), 2) for k in all_keys}

        def calc_metric_diff(metrics_a, metrics_b, key):
            val_a = metrics_a.get(key)
            val_b = metrics_b.get(key)
            if val_a is not None and val_b is not None:
                return round(val_b - val_a, 2)
            return None

        impact = {
            'allocation_change': calc_diff(original_analysis['asset_allocation'], modified_analysis['asset_allocation']),
            'sector_change': calc_diff(original_analysis['sector_exposure'], modified_analysis['sector_exposure']),
            'geography_change': calc_diff(original_analysis['geography'], modified_analysis['geography']),
            'risk_change': {
                'volatility': calc_metric_diff(original_analysis['risk_metrics'], modified_analysis['risk_metrics'], 'volatility'),
                'beta': calc_metric_diff(original_analysis['risk_metrics'], modified_analysis['risk_metrics'], 'beta'),
                'sharpe_ratio': calc_metric_diff(original_analysis['risk_metrics'], modified_analysis['risk_metrics'], 'sharpe_ratio'),
                'max_drawdown': calc_metric_diff(original_analysis['risk_metrics'], modified_analysis['risk_metrics'], 'max_drawdown')
            },
            'value_change': round((modified_analysis['total_value'] or 0) - (original_analysis['total_value'] or 0), 2),
            'cost_to_execute': round(total_cost, 2)
        }

        return jsonify({
            'original': original_analysis,
            'modified': modified_analysis,
            'impact': impact
        })

    except Exception as e:
        return jsonify({'error': f'What-if analysis failed: {str(e)}'}), 500


def generate_portfolio_insights(allocations, concentration, risk_metrics, positions, projections):
    """Generate plain English insights about the portfolio."""
    insights = []

    total_value = allocations.get('total_value', 0)
    asset_alloc = allocations.get('asset_allocation', {})
    sector_exp = allocations.get('sector_exposure', {})
    geography = allocations.get('geography', {})

    # Portfolio size insight
    if total_value >= 1000000:
        insights.append({
            'category': 'overview',
            'title': 'Portfolio Size',
            'text': f"Your portfolio is valued at ${total_value:,.0f}, placing you in the high-net-worth category. Consider consulting a financial advisor for tax-optimized strategies.",
            'type': 'info'
        })
    elif total_value >= 100000:
        insights.append({
            'category': 'overview',
            'title': 'Portfolio Size',
            'text': f"Your portfolio of ${total_value:,.0f} is well-established. Focus on maintaining diversification and managing risk as your wealth grows.",
            'type': 'info'
        })
    else:
        insights.append({
            'category': 'overview',
            'title': 'Portfolio Size',
            'text': f"Your portfolio is valued at ${total_value:,.0f}. At this stage, focus on consistent contributions and keeping investment costs low.",
            'type': 'info'
        })

    # Asset allocation insights
    stocks = asset_alloc.get('Stocks', 0)
    bonds = asset_alloc.get('Bonds', 0)
    cash = asset_alloc.get('Cash', 0)

    if stocks > 90:
        insights.append({
            'category': 'allocation',
            'title': 'High Stock Concentration',
            'text': f"Your portfolio is {stocks:.0f}% stocks, which is aggressive. This can lead to significant volatility during market downturns. Consider adding bonds or other assets for stability, especially if you're within 10 years of retirement.",
            'type': 'warning'
        })
    elif stocks > 70:
        insights.append({
            'category': 'allocation',
            'title': 'Growth-Oriented Portfolio',
            'text': f"With {stocks:.0f}% in stocks and {bonds:.0f}% in bonds, your portfolio is positioned for growth. This is appropriate if you have a long time horizon (10+ years) and can tolerate market swings.",
            'type': 'info'
        })
    elif stocks > 40:
        insights.append({
            'category': 'allocation',
            'title': 'Balanced Portfolio',
            'text': f"Your {stocks:.0f}% stocks / {bonds:.0f}% bonds allocation is balanced. This provides a mix of growth potential and stability, suitable for moderate risk tolerance.",
            'type': 'positive'
        })
    elif bonds > 50:
        insights.append({
            'category': 'allocation',
            'title': 'Conservative Portfolio',
            'text': f"With {bonds:.0f}% in bonds, your portfolio is conservative. This provides stability but may not keep pace with inflation long-term. If you have many years until retirement, consider increasing stock exposure.",
            'type': 'info'
        })

    if cash > 15:
        insights.append({
            'category': 'allocation',
            'title': 'High Cash Position',
            'text': f"You're holding {cash:.0f}% in cash, which is above typical recommendations (3-6 months expenses). While cash provides safety, it loses value to inflation over time. Consider deploying excess cash into diversified investments.",
            'type': 'warning'
        })

    # Concentration risk
    top_holdings = concentration.get('top_10_weight', 0)
    if top_holdings > 50:
        top_names = [h.get('symbol', 'Unknown') for h in concentration.get('top_holdings', [])[:3]]
        insights.append({
            'category': 'risk',
            'title': 'Concentration Risk',
            'text': f"Your top 10 holdings represent {top_holdings:.0f}% of your portfolio, with heavy positions in {', '.join(top_names)}. Consider diversifying to reduce single-stock risk.",
            'type': 'warning'
        })
    elif top_holdings < 30:
        insights.append({
            'category': 'risk',
            'title': 'Well Diversified',
            'text': f"Your top 10 holdings represent just {top_holdings:.0f}% of your portfolio. This diversification helps protect against individual stock volatility.",
            'type': 'positive'
        })

    # Sector concentration
    if sector_exp:
        top_sector = max(sector_exp.items(), key=lambda x: x[1]) if sector_exp else ('N/A', 0)
        if top_sector[1] > 40:
            insights.append({
                'category': 'sectors',
                'title': f'Heavy {top_sector[0]} Exposure',
                'text': f"You have {top_sector[1]:.0f}% of your portfolio in {top_sector[0]}. While sector bets can pay off, they also increase risk if that sector underperforms. The S&P 500 has about 30% in Technology for comparison.",
                'type': 'warning'
            })

    # Geographic diversification
    us_exposure = geography.get('US', 0)
    if us_exposure > 90:
        insights.append({
            'category': 'geography',
            'title': 'US-Concentrated Portfolio',
            'text': f"Your portfolio is {us_exposure:.0f}% US-based. While US markets have performed well historically, international diversification can reduce risk and capture global growth opportunities. Consider allocating 20-40% internationally.",
            'type': 'info'
        })
    elif us_exposure < 50:
        intl = 100 - us_exposure
        insights.append({
            'category': 'geography',
            'title': 'Strong International Exposure',
            'text': f"With {intl:.0f}% international exposure, you're well-diversified globally. This can provide access to faster-growing economies, though it may also add currency risk.",
            'type': 'positive'
        })

    # Risk metrics insights
    if risk_metrics:
        volatility = risk_metrics.get('volatility')
        beta = risk_metrics.get('beta')
        sharpe = risk_metrics.get('sharpe_ratio')

        if volatility and volatility > 20:
            insights.append({
                'category': 'risk',
                'title': 'High Volatility',
                'text': f"Your portfolio's volatility is {volatility:.1f}%, higher than the typical 15-18% for a balanced portfolio. This means larger swings in value—both up and down. Ensure this matches your risk tolerance.",
                'type': 'warning'
            })
        elif volatility and volatility < 12:
            insights.append({
                'category': 'risk',
                'title': 'Low Volatility',
                'text': f"Your portfolio's volatility of {volatility:.1f}% is relatively low, suggesting a conservative approach. This provides stability but may limit long-term growth potential.",
                'type': 'info'
            })

        if beta and beta > 1.2:
            insights.append({
                'category': 'risk',
                'title': 'High Market Sensitivity',
                'text': f"Your portfolio beta of {beta:.2f} means it tends to move more than the market. When stocks rise 10%, you might gain 12%+, but losses are amplified too.",
                'type': 'warning'
            })
        elif beta and beta < 0.8:
            insights.append({
                'category': 'risk',
                'title': 'Defensive Portfolio',
                'text': f"With a beta of {beta:.2f}, your portfolio is less sensitive to market movements. This can provide protection during downturns but may lag in strong bull markets.",
                'type': 'info'
            })

        if sharpe and sharpe > 1:
            insights.append({
                'category': 'risk',
                'title': 'Strong Risk-Adjusted Returns',
                'text': f"Your Sharpe ratio of {sharpe:.2f} indicates you're earning good returns relative to the risk you're taking. A ratio above 1 is generally considered good.",
                'type': 'positive'
            })
        elif sharpe and sharpe < 0.5:
            insights.append({
                'category': 'risk',
                'title': 'Low Risk-Adjusted Returns',
                'text': f"Your Sharpe ratio of {sharpe:.2f} suggests the returns may not be compensating adequately for the risk. Consider whether adjusting your allocation could improve this.",
                'type': 'warning'
            })

    # Projections insight
    if projections and projections.get('monte_carlo'):
        mc = projections['monte_carlo']
        summary = mc.get('summary', {})
        prob_gain = summary.get('prob_gain', 0)
        median = summary.get('median', 0)

        if prob_gain and median:
            insights.append({
                'category': 'projections',
                'title': '10-Year Outlook',
                'text': f"Based on Monte Carlo simulation, there's a {prob_gain:.0f}% chance your portfolio grows over the next 10 years. The median projected value is ${median:,.0f}. Remember, past performance doesn't guarantee future results.",
                'type': 'info'
            })

    # Number of holdings insight
    num_holdings = len(positions)
    if num_holdings < 5:
        insights.append({
            'category': 'overview',
            'title': 'Limited Holdings',
            'text': f"You have only {num_holdings} holdings. Consider adding more positions or using diversified ETFs to reduce individual stock risk.",
            'type': 'warning'
        })
    elif num_holdings > 50:
        insights.append({
            'category': 'overview',
            'title': 'Many Holdings',
            'text': f"With {num_holdings} holdings, your portfolio is well-diversified but may be complex to manage. Consider consolidating into broad ETFs for simplicity while maintaining diversification.",
            'type': 'info'
        })

    return insights


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
            projections = calculate_projections(positions, allocations)
        else:
            risk_metrics = {
                'volatility': None,
                'beta': None,
                'sharpe_ratio': None,
                'max_drawdown': None
            }
            historical_performance = {'returns': {}, 'chart_data': None}
            projections = {'capital_market_assumptions': {}, 'monte_carlo': None}

        # Calculate scenario analysis
        scenario_analysis = calculate_scenario_analysis(positions, allocations)

        # Add classification to each position
        classified_positions = []
        for pos in positions:
            classified_pos = pos.copy()
            classification = get_classification(pos.get('symbol', ''))
            classified_pos['classification'] = classification
            classified_positions.append(classified_pos)

        # Generate plain English insights
        insights = generate_portfolio_insights(
            allocations, concentration, risk_metrics, classified_positions, projections
        )

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
            'historical_performance': historical_performance,
            'scenario_analysis': scenario_analysis,
            'projections': projections,
            'insights': insights
        })

    except Exception as e:
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500


# =============================================================================
# PDF REPORT GENERATION
# =============================================================================

def generate_report_html(data, report_date):
    """Generate professional HTML for the portfolio report - WeasyPrint compatible."""

    total_value = data.get('total_value', 0)
    positions = data.get('positions', [])
    asset_allocation = data.get('asset_allocation', {})
    sub_asset_allocation = data.get('sub_asset_allocation', {})
    sector_exposure = data.get('sector_exposure', {})
    sector_benchmark = data.get('sector_benchmark', {})
    geography = data.get('geography', {})
    concentration = data.get('concentration', {})
    risk_metrics = data.get('risk_metrics', {})
    insights = data.get('insights', [])
    projections = data.get('projections', {})
    historical_performance = data.get('historical_performance', {})
    benchmark_comparison = data.get('benchmark_comparison', {})

    # Format helpers
    def fmt_currency(val):
        if val is None:
            return 'N/A'
        return f"${val:,.2f}"

    def fmt_pct(val):
        if val is None:
            return 'N/A'
        return f"{val:.1f}%"

    def fmt_pct_change(val):
        if val is None:
            return 'N/A'
        sign = '+' if val >= 0 else ''
        return f"{sign}{val:.1f}%"

    # Risk metrics
    volatility = risk_metrics.get('volatility')
    beta = risk_metrics.get('beta')
    sharpe = risk_metrics.get('sharpe_ratio')
    max_dd = risk_metrics.get('max_drawdown')

    # Monte Carlo projections
    mc_data = projections.get('monte_carlo', {})
    mc_summary = mc_data.get('summary', {})

    # Monte Carlo additional metrics
    mc_prob_gain = mc_data.get('probability_of_gain')
    mc_chance_double = mc_data.get('chance_to_double')
    mc_implied_cagr = mc_data.get('implied_cagr')
    mc_simulations = mc_data.get('simulations', 1000)

    # Colors for charts
    colors = ['#635bff', '#30d158', '#ff9f0a', '#ff453a', '#5e5ce6', '#64d2ff', '#bf5af2', '#ff375f']

    # Build stacked bar chart for asset allocation (visual pie chart alternative)
    allocation_sorted = sorted(asset_allocation.items(), key=lambda x: x[1], reverse=True)
    allocation_bar_segments = ""
    for i, (asset_class, pct) in enumerate(allocation_sorted):
        if pct > 0:
            color = colors[i % len(colors)]
            allocation_bar_segments += f'<td style="width: {pct}%; background: {color}; padding: 0; border: none;"></td>'

    # Build allocation legend
    allocation_legend_items = ""
    for i, (asset_class, pct) in enumerate(allocation_sorted):
        if pct > 0:
            color = colors[i % len(colors)]
            value = total_value * pct / 100 if total_value else 0
            allocation_legend_items += f'''
                <tr>
                    <td style="width: 16px; padding: 4px;"><div style="width: 12px; height: 12px; background: {color}; border-radius: 2px;"></div></td>
                    <td style="padding: 4px; font-weight: 500;">{asset_class}</td>
                    <td style="padding: 4px; text-align: right; font-weight: 600;">{pct:.1f}%</td>
                    <td style="padding: 4px; text-align: right; color: #666;">{fmt_currency(value)}</td>
                </tr>
            '''

    # Build stacked bar for sector exposure
    sector_sorted = sorted(sector_exposure.items(), key=lambda x: x[1], reverse=True)[:8]
    sector_bar_segments = ""
    sector_legend_items = ""
    sector_total = sum(pct for _, pct in sector_sorted if pct > 0)
    for i, (sector, pct) in enumerate(sector_sorted):
        if pct > 0:
            color = colors[i % len(colors)]
            # Normalize to 100% for visual bar
            bar_pct = (pct / sector_total * 100) if sector_total else 0
            sector_bar_segments += f'<td style="width: {bar_pct}%; background: {color}; padding: 0; border: none;"></td>'
            sector_legend_items += f'''
                <tr>
                    <td style="width: 12px; padding: 3px;"><div style="width: 8px; height: 8px; background: {color}; border-radius: 1px;"></div></td>
                    <td style="padding: 3px; font-size: 8px;">{sector}</td>
                    <td style="padding: 3px; text-align: right; font-size: 8px; font-weight: 600;">{pct:.1f}%</td>
                </tr>
            '''

    # Build stacked bar for geography
    geo_sorted = sorted(geography.items(), key=lambda x: x[1], reverse=True)[:6]
    geo_bar_segments = ""
    geo_legend_items = ""
    geo_colors = ['#ff9f0a', '#ffcc02', '#ff6b35', '#e85d04', '#dc2f02', '#9d0208']
    geo_total = sum(pct for _, pct in geo_sorted if pct > 0)
    for i, (region, pct) in enumerate(geo_sorted):
        if pct > 0:
            color = geo_colors[i % len(geo_colors)]
            # Normalize to 100% for visual bar
            bar_pct = (pct / geo_total * 100) if geo_total else 0
            geo_bar_segments += f'<td style="width: {bar_pct}%; background: {color}; padding: 0; border: none;"></td>'
            geo_legend_items += f'''
                <tr>
                    <td style="width: 12px; padding: 3px;"><div style="width: 8px; height: 8px; background: {color}; border-radius: 1px;"></div></td>
                    <td style="padding: 3px; font-size: 8px;">{region}</td>
                    <td style="padding: 3px; text-align: right; font-size: 8px; font-weight: 600;">{pct:.1f}%</td>
                </tr>
            '''

    # Build historical performance rows
    perf_periods = ['1M', '3M', '6M', 'YTD', '1Y', '3Y', '5Y']
    perf_rows = ""
    for period in perf_periods:
        port_val = historical_performance.get(period)
        bench_val = benchmark_comparison.get(period)
        port_color = '#30d158' if port_val and port_val >= 0 else '#ff453a'
        bench_color = '#30d158' if bench_val and bench_val >= 0 else '#ff453a'
        perf_rows += f'''
            <td style="text-align: center; padding: 8px; background: #f8f9fc; border-right: 2px solid white;">
                <div style="font-size: 9px; color: #666; margin-bottom: 4px; font-weight: 600;">{period}</div>
                <div style="font-size: 16px; font-weight: 700; color: {port_color};">{fmt_pct_change(port_val) if port_val is not None else 'N/A'}</div>
                <div style="font-size: 8px; color: {bench_color}; margin-top: 2px;">S&P: {fmt_pct_change(bench_val) if bench_val is not None else 'N/A'}</div>
            </td>
        '''

    # Build sub-asset allocation rows
    sub_asset_rows = ""
    if sub_asset_allocation:
        sub_sorted = sorted(sub_asset_allocation.items(), key=lambda x: x[1], reverse=True)
        max_sub = sub_sorted[0][1] if sub_sorted else 100
        for i, (sub_class, pct) in enumerate(sub_sorted):
            if pct > 0:
                color = colors[i % len(colors)]
                value = total_value * pct / 100 if total_value else 0
                bar_width = (pct / max_sub) * 100
                sub_asset_rows += f'''
                    <tr>
                        <td style="width: 16px;"><div style="width: 10px; height: 10px; background: {color}; border-radius: 2px;"></div></td>
                        <td style="font-size: 8px;">{sub_class}</td>
                        <td class="num" style="font-size: 8px;">{pct:.1f}%</td>
                        <td class="num" style="font-size: 8px;">{fmt_currency(value)}</td>
                        <td style="width: 30%;">
                            <div style="background: #f0f0f0; height: 8px; border-radius: 2px;">
                                <div style="background: {color}; height: 8px; width: {bar_width}%; border-radius: 2px;"></div>
                            </div>
                        </td>
                    </tr>
                '''

    # Build Top 10 holdings rows
    top10_rows = ""
    sorted_positions = sorted(positions, key=lambda x: x.get('value', 0), reverse=True)
    for i, pos in enumerate(sorted_positions[:10]):
        symbol = pos.get('symbol', 'N/A')
        description = pos.get('description', '')[:25]
        value = pos.get('value', 0)
        pct = (value / total_value * 100) if total_value else 0
        bar_width = min(pct * 2, 100)
        top10_rows += f'''
            <tr>
                <td style="text-align: center; font-weight: 700; color: #635bff; width: 30px;">{i + 1}</td>
                <td><strong>{symbol}</strong><br/><span style="font-size: 7px; color: #888;">{description}</span></td>
                <td class="num">{fmt_currency(value)}</td>
                <td class="num">{pct:.2f}%</td>
                <td style="width: 25%;">
                    <div style="background: #e8eaf6; height: 10px; border-radius: 2px;">
                        <div style="background: #635bff; height: 10px; width: {bar_width}%; border-radius: 2px;"></div>
                    </div>
                </td>
            </tr>
        '''

    # Build sector comparison rows (portfolio vs benchmark)
    sp500_sectors = {
        'Technology': 30.0, 'Healthcare': 13.0, 'Financials': 12.5, 'Consumer Discretionary': 10.5,
        'Communication Services': 8.5, 'Industrials': 8.0, 'Consumer Staples': 6.5,
        'Energy': 4.0, 'Utilities': 2.5, 'Real Estate': 2.5, 'Materials': 2.0
    }
    sector_compare_rows = ""
    all_sectors = set(sector_exposure.keys()) | set(sector_benchmark.keys() if sector_benchmark else sp500_sectors.keys())
    sector_data = []
    for sector in all_sectors:
        port_pct = sector_exposure.get(sector, 0)
        bench_pct = sector_benchmark.get(sector, sp500_sectors.get(sector, 0)) if sector_benchmark else sp500_sectors.get(sector, 0)
        if port_pct > 0 or bench_pct > 0:
            sector_data.append((sector, port_pct, bench_pct))
    sector_data.sort(key=lambda x: x[1], reverse=True)
    max_sector_pct = max([max(s[1], s[2]) for s in sector_data]) if sector_data else 100

    for sector, port_pct, bench_pct in sector_data[:10]:
        port_width = (port_pct / max_sector_pct) * 100 if max_sector_pct else 0
        bench_width = (bench_pct / max_sector_pct) * 100 if max_sector_pct else 0
        diff = port_pct - bench_pct
        diff_color = '#30d158' if diff >= 0 else '#ff453a'
        sector_compare_rows += f'''
            <tr>
                <td style="font-size: 8px;">{sector}</td>
                <td style="width: 22%;">
                    <div style="background: #e8eaf6; height: 12px; border-radius: 2px;">
                        <div style="background: #635bff; height: 12px; width: {port_width}%; border-radius: 2px;"></div>
                    </div>
                </td>
                <td class="num" style="font-size: 8px;">{port_pct:.1f}%</td>
                <td style="width: 22%;">
                    <div style="background: #fff3e0; height: 12px; border-radius: 2px;">
                        <div style="background: #ff9f0a; height: 12px; width: {bench_width}%; border-radius: 2px;"></div>
                    </div>
                </td>
                <td class="num" style="font-size: 8px;">{bench_pct:.1f}%</td>
                <td class="num" style="font-size: 9px; font-weight: 600; color: {diff_color};">{diff:+.1f}%</td>
            </tr>
        '''

    # Build allocation table rows with horizontal bars (for detailed view)
    max_alloc = allocation_sorted[0][1] if allocation_sorted else 100
    allocation_rows = ""
    for i, (asset_class, pct) in enumerate(allocation_sorted):
        if pct > 0:
            color = colors[i % len(colors)]
            value = total_value * pct / 100 if total_value else 0
            bar_width = (pct / max_alloc) * 100
            allocation_rows += f'''
                <tr>
                    <td style="width: 20px;"><div style="width: 14px; height: 14px; background: {color}; border-radius: 3px;"></div></td>
                    <td style="font-weight: 600;">{asset_class}</td>
                    <td class="number">{pct:.1f}%</td>
                    <td class="number">{fmt_currency(value)}</td>
                    <td style="width: 40%;">
                        <div style="background: #f0f0f0; height: 12px; border-radius: 2px;">
                            <div style="background: {color}; height: 12px; width: {bar_width}%; border-radius: 2px;"></div>
                        </div>
                    </td>
                </tr>
            '''

    # Build sector table rows
    sector_sorted = sorted(sector_exposure.items(), key=lambda x: x[1], reverse=True)[:8]
    max_sector = sector_sorted[0][1] if sector_sorted else 100
    sector_rows = ""
    for sector, pct in sector_sorted:
        if pct > 0:
            bar_width = (pct / max_sector) * 100
            sector_rows += f'''
                <tr>
                    <td>{sector}</td>
                    <td style="width: 50%;">
                        <div style="background: #e8f5e9; height: 14px; border-radius: 2px;">
                            <div style="background: #30d158; height: 14px; width: {bar_width}%; border-radius: 2px;"></div>
                        </div>
                    </td>
                    <td class="number" style="font-weight: 600;">{pct:.1f}%</td>
                </tr>
            '''

    # Build geography table rows
    geo_sorted = sorted(geography.items(), key=lambda x: x[1], reverse=True)[:6]
    max_geo = geo_sorted[0][1] if geo_sorted else 100
    geo_rows = ""
    for region, pct in geo_sorted:
        if pct > 0:
            bar_width = (pct / max_geo) * 100
            geo_rows += f'''
                <tr>
                    <td>{region}</td>
                    <td style="width: 50%;">
                        <div style="background: #fff3e0; height: 14px; border-radius: 2px;">
                            <div style="background: #ff9f0a; height: 14px; width: {bar_width}%; border-radius: 2px;"></div>
                        </div>
                    </td>
                    <td class="number" style="font-weight: 600;">{pct:.1f}%</td>
                </tr>
            '''

    # Build positions table rows - show ALL positions
    positions_rows = ""
    sorted_positions = sorted(positions, key=lambda x: x.get('value', 0), reverse=True)
    for i, pos in enumerate(sorted_positions):
        symbol = pos.get('symbol', 'N/A')
        description = pos.get('description', '')[:40]
        shares = pos.get('shares', 0)
        price = pos.get('price', 0)
        value = pos.get('value', 0)
        pct = (value / total_value * 100) if total_value else 0
        bg_color = '#fafafa' if i % 2 == 1 else '#ffffff'
        bar_width = min(pct * 1.8, 100)
        positions_rows += f'''
            <tr style="background: {bg_color};">
                <td>
                    <strong style="font-size: 10px;">{symbol}</strong><br/>
                    <span style="font-size: 8px; color: #666;">{description}</span>
                </td>
                <td class="number">{shares:,.4f}</td>
                <td class="number">{fmt_currency(price)}</td>
                <td class="number">{fmt_currency(value)}</td>
                <td class="number">
                    {pct:.2f}%
                    <div style="background: #e8eaf6; height: 4px; margin-top: 3px; border-radius: 2px;">
                        <div style="background: #635bff; height: 4px; width: {bar_width}%; border-radius: 2px;"></div>
                    </div>
                </td>
            </tr>
        '''

    # Build insights rows (2-column table layout)
    insight_colors = {
        'info': '#635bff',
        'warning': '#ff9f0a',
        'success': '#30d158',
        'positive': '#30d158',
        'negative': '#ff453a',
    }
    insights_rows = ""
    for i in range(0, len(insights[:8]), 2):
        insights_rows += "<tr>"
        for j in range(2):
            if i + j < len(insights[:8]):
                insight = insights[i + j]
                border_color = insight_colors.get(insight.get('type', 'info'), '#635bff')
                insights_rows += f'''
                    <td style="width: 50%; vertical-align: top; padding: 8px;">
                        <div style="background: #fafafa; border-left: 4px solid {border_color}; padding: 12px; border-radius: 0 4px 4px 0;">
                            <div style="font-weight: 600; font-size: 10px; margin-bottom: 6px;">{insight.get('title', '')}</div>
                            <div style="font-size: 8px; color: #555; line-height: 1.5;">{insight.get('text', '')}</div>
                        </div>
                    </td>
                '''
            else:
                insights_rows += "<td></td>"
        insights_rows += "</tr>"

    # Risk level calculation for gauge
    def get_risk_label(vol):
        if vol is None:
            return "Moderate", 50
        if vol < 8:
            return "Very Conservative", 10
        elif vol < 12:
            return "Conservative", 25
        elif vol < 16:
            return "Moderate", 50
        elif vol < 22:
            return "Aggressive", 75
        else:
            return "Very Aggressive", 90

    risk_label, risk_pct = get_risk_label(volatility)

    # Scenario analysis data
    scenarios = [
        {'name': '2008 Financial Crisis', 'market': -37.0, 'portfolio': round(-37.0 * (beta or 1), 1)},
        {'name': '2020 COVID Crash', 'market': -33.9, 'portfolio': round(-33.9 * (beta or 1), 1)},
        {'name': '2022 Bear Market', 'market': -18.1, 'portfolio': round(-18.1 * (beta or 1), 1)},
        {'name': 'Interest Rate +2%', 'market': -8.0, 'portfolio': round(-8.0 * (beta or 1), 1)},
        {'name': 'Recession', 'market': -25.0, 'portfolio': round(-25.0 * (beta or 1), 1)},
    ]

    scenarios_rows = ""
    for scenario in scenarios:
        mkt_color = '#ff453a' if scenario['market'] < 0 else '#30d158'
        port_color = '#ff453a' if scenario['portfolio'] < 0 else '#30d158'
        scenarios_rows += f'''
            <tr>
                <td>{scenario['name']}</td>
                <td class="number" style="color: {mkt_color};">{scenario['market']:+.1f}%</td>
                <td class="number" style="color: {port_color};">{scenario['portfolio']:+.1f}%</td>
                <td class="number">{fmt_currency(total_value * (1 + scenario['portfolio']/100)) if total_value else 'N/A'}</td>
            </tr>
        '''

    # Monte Carlo projections table
    mc_rows = ""
    if mc_summary:
        for year in [1, 5, 10]:
            year_data = mc_summary.get(f'year_{year}', {})
            if year_data:
                mc_rows += f'''
                    <tr>
                        <td>Year {year}</td>
                        <td class="number">{fmt_currency(year_data.get('percentile_10'))}</td>
                        <td class="number">{fmt_currency(year_data.get('median'))}</td>
                        <td class="number">{fmt_currency(year_data.get('percentile_90'))}</td>
                    </tr>
                '''

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Portfolio Analysis Report</title>
        <style>
            /* Page Setup with proper footer positioning */
            @page {{
                size: letter;
                margin: 0.7in 0.7in 1in 0.7in;

                @bottom-left {{
                    content: "Statement Scan";
                    font-family: Helvetica, Arial, sans-serif;
                    font-size: 8px;
                    color: #666;
                }}

                @bottom-center {{
                    content: "Page " counter(page) " of " counter(pages);
                    font-family: Helvetica, Arial, sans-serif;
                    font-size: 8px;
                    color: #666;
                }}

                @bottom-right {{
                    content: "{report_date}";
                    font-family: Helvetica, Arial, sans-serif;
                    font-size: 8px;
                    color: #666;
                }}
            }}

            @page :first {{
                margin: 0;
                @bottom-left {{ content: none; }}
                @bottom-center {{ content: none; }}
                @bottom-right {{ content: none; }}
            }}

            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: Helvetica, Arial, sans-serif;
                font-size: 9pt;
                line-height: 1.4;
                color: #1a1a2e;
            }}

            /* Cover Page */
            .cover-page {{
                page-break-after: always;
                height: 100%;
            }}

            .cover-header {{
                background: #635bff;
                padding: 50px;
                color: white;
            }}

            .cover-logo-row {{
                margin-bottom: 50px;
            }}

            .cover-logo-icon {{
                display: inline-block;
                width: 44px;
                height: 44px;
                background: rgba(255,255,255,0.2);
                border-radius: 8px;
                vertical-align: middle;
                margin-right: 12px;
                text-align: center;
                line-height: 44px;
                font-size: 24px;
            }}

            .cover-brand {{
                display: inline-block;
                vertical-align: middle;
                font-size: 22px;
                font-weight: 700;
            }}

            .cover-title {{
                font-size: 38px;
                font-weight: 300;
                margin-bottom: 8px;
            }}

            .cover-subtitle {{
                font-size: 14px;
                opacity: 0.9;
            }}

            .cover-body {{
                padding: 40px 50px;
            }}

            .cover-stats {{
                width: 100%;
                border-collapse: collapse;
            }}

            .cover-stats td {{
                width: 50%;
                padding: 25px 0;
                border-bottom: 1px solid #e5e5e5;
                vertical-align: top;
            }}

            .cover-stat-label {{
                font-size: 10px;
                color: #666;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 8px;
            }}

            .cover-stat-value {{
                font-size: 28px;
                font-weight: 700;
                color: #1a1a2e;
            }}

            .cover-stat-value.primary {{
                color: #635bff;
            }}

            .cover-footer {{
                position: absolute;
                bottom: 40px;
                left: 50px;
                right: 50px;
                padding-top: 20px;
                border-top: 1px solid #e5e5e5;
                font-size: 9px;
                color: #666;
            }}

            .cover-footer-left {{
                float: left;
            }}

            .cover-footer-right {{
                float: right;
            }}

            /* Page Header */
            .page-header {{
                border-bottom: 2px solid #635bff;
                padding-bottom: 8px;
                margin-bottom: 16px;
            }}

            .page-header-logo {{
                display: inline-block;
                width: 18px;
                height: 18px;
                background: #635bff;
                border-radius: 3px;
                vertical-align: middle;
                margin-right: 8px;
            }}

            .page-header-brand {{
                display: inline-block;
                vertical-align: middle;
                font-size: 11px;
                font-weight: 600;
            }}

            .page-header-title {{
                float: right;
                font-size: 9px;
                color: #666;
                line-height: 18px;
            }}

            /* Content Sections */
            .content-page {{
                page-break-before: always;
            }}

            .section-header {{
                background: #f8f9fc;
                padding: 8px 12px;
                margin: 14px 0 10px 0;
                border-left: 4px solid #635bff;
            }}

            .section-header h2 {{
                font-size: 12px;
                font-weight: 700;
                color: #1a1a2e;
                text-transform: uppercase;
                letter-spacing: 0.3px;
                margin: 0;
            }}

            /* Commentary text */
            .commentary {{
                font-size: 8px;
                color: #555;
                line-height: 1.5;
                margin-bottom: 12px;
                padding: 8px 10px;
                background: #fafafa;
                border-radius: 4px;
            }}

            /* Stacked bar chart */
            .stacked-bar {{
                height: 24px;
                border-radius: 4px;
                overflow: hidden;
                display: flex;
                margin-bottom: 10px;
            }}

            /* Metrics */
            .metrics-table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 16px;
            }}

            .metrics-table td {{
                width: 25%;
                padding: 14px;
                background: #f8f9fc;
                text-align: center;
                vertical-align: top;
            }}

            .metrics-table td + td {{
                border-left: 3px solid white;
            }}

            .metric-label {{
                font-size: 8px;
                color: #666;
                text-transform: uppercase;
                letter-spacing: 0.3px;
                margin-bottom: 6px;
            }}

            .metric-value {{
                font-size: 18px;
                font-weight: 700;
                color: #1a1a2e;
            }}

            .metric-subtext {{
                font-size: 7px;
                color: #888;
                margin-top: 4px;
            }}

            /* Tables */
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 9px;
            }}

            th {{
                background: #f8f9fc;
                padding: 8px;
                text-align: left;
                font-weight: 600;
                color: #666;
                text-transform: uppercase;
                font-size: 8px;
                letter-spacing: 0.3px;
                border-bottom: 2px solid #e5e5e5;
            }}

            td {{
                padding: 8px;
                border-bottom: 1px solid #f0f0f0;
                vertical-align: middle;
            }}

            .number {{
                text-align: right;
            }}

            /* Two Column */
            .two-col-table {{
                width: 100%;
                border-collapse: collapse;
            }}

            .two-col-table > tbody > tr > td {{
                width: 50%;
                vertical-align: top;
                padding: 0;
            }}

            .two-col-table > tbody > tr > td:first-child {{
                padding-right: 12px;
            }}

            .two-col-table > tbody > tr > td:last-child {{
                padding-left: 12px;
            }}

            /* Risk Gauge - Simple bar version */
            .risk-gauge-simple {{
                margin: 20px 0;
                text-align: center;
            }}

            .risk-bar-container {{
                width: 100%;
                height: 24px;
                background: linear-gradient(to right, #30d158 0%, #ffd60a 40%, #ff9f0a 70%, #ff453a 100%);
                border-radius: 12px;
                position: relative;
                margin-bottom: 8px;
            }}

            .risk-indicator {{
                position: absolute;
                top: -4px;
                width: 8px;
                height: 32px;
                background: #1a1a2e;
                border-radius: 4px;
                border: 2px solid white;
            }}

            .risk-labels-row {{
                width: 100%;
            }}

            .risk-labels-row td {{
                font-size: 8px;
                color: #666;
                border: none;
                padding: 4px 0;
            }}

            /* Glossary */
            .glossary-table {{
                width: 100%;
            }}

            .glossary-table td {{
                width: 50%;
                vertical-align: top;
                padding: 0 10px 0 0;
            }}

            .glossary-term {{
                margin-bottom: 14px;
            }}

            .glossary-term-title {{
                font-weight: 600;
                font-size: 9px;
                color: #1a1a2e;
                margin-bottom: 3px;
            }}

            .glossary-term-def {{
                font-size: 8px;
                color: #666;
                line-height: 1.5;
            }}

            /* Disclosures */
            .disclosures {{
                background: #f8f9fc;
                padding: 16px;
                margin-top: 20px;
            }}

            .disclosures h3 {{
                font-size: 10px;
                font-weight: 600;
                margin-bottom: 10px;
            }}

            .disclosures p {{
                font-size: 7px;
                color: #666;
                line-height: 1.6;
                margin-bottom: 8px;
            }}
        </style>
    </head>
    <body>
        <!-- Cover Page -->
        <div class="cover-page">
            <div class="cover-header">
                <div class="cover-logo-row">
                    <span class="cover-logo-icon">&#9632;</span>
                    <span class="cover-brand">Statement Scan</span>
                </div>
                <div class="cover-title">Portfolio Analysis Report</div>
                <div class="cover-subtitle">Comprehensive investment analysis and insights</div>
            </div>

            <div class="cover-body">
                <table class="cover-stats">
                    <tr>
                        <td>
                            <div class="cover-stat-label">Total Portfolio Value</div>
                            <div class="cover-stat-value primary">{fmt_currency(total_value)}</div>
                        </td>
                        <td>
                            <div class="cover-stat-label">Number of Holdings</div>
                            <div class="cover-stat-value">{len(positions)}</div>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            <div class="cover-stat-label">Portfolio Volatility</div>
                            <div class="cover-stat-value">{fmt_pct(volatility) if volatility else 'N/A'}</div>
                        </td>
                        <td>
                            <div class="cover-stat-label">Risk-Adjusted Return (Sharpe)</div>
                            <div class="cover-stat-value">{f'{sharpe:.2f}' if sharpe else 'N/A'}</div>
                        </td>
                    </tr>
                </table>

                <div class="cover-footer">
                    <span class="cover-footer-left">Report Generated: {report_date}</span>
                    <span class="cover-footer-right">Statement Scan Portfolio Intelligence</span>
                </div>
            </div>
        </div>

        <!-- Page 2: Executive Overview & Performance -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>Executive Overview</h2>
            </div>

            <div class="commentary">
                This portfolio analysis provides a comprehensive view of your investment holdings, including asset allocation,
                risk metrics, sector exposure, and projected future values. Understanding these metrics helps ensure your
                investments align with your financial goals and risk tolerance.
            </div>

            <table class="metrics-table">
                <tr>
                    <td>
                        <div class="metric-label">Total Value</div>
                        <div class="metric-value">{fmt_currency(total_value)}</div>
                    </td>
                    <td>
                        <div class="metric-label">Holdings</div>
                        <div class="metric-value">{len(positions)}</div>
                    </td>
                    <td>
                        <div class="metric-label">Top 10 Concentration</div>
                        <div class="metric-value">{fmt_pct(concentration.get('top_10_weight'))}</div>
                    </td>
                    <td>
                        <div class="metric-label">Largest Position</div>
                        <div class="metric-value">{fmt_pct(concentration.get('largest_weight'))}</div>
                    </td>
                </tr>
            </table>

            <div class="section-header">
                <h2>Historical Performance</h2>
            </div>

            <div class="commentary">
                Historical returns show how your portfolio has performed compared to the S&amp;P 500 benchmark.
                Performance across different time periods helps identify trends and evaluate whether your investment strategy
                is achieving expected results. Past performance does not guarantee future results.
            </div>

            <table style="margin-bottom: 20px; border-collapse: separate; border-spacing: 4px;">
                <tr>
                    {perf_rows}
                </tr>
            </table>

            <div class="section-header">
                <h2>Asset Allocation</h2>
            </div>

            <div class="commentary">
                Asset allocation is one of the most important factors in determining portfolio risk and return.
                A diversified mix of stocks, bonds, and other asset classes helps manage risk while pursuing growth.
                The allocation below shows how your portfolio is distributed across major asset categories.
            </div>

            <!-- Visual Allocation Bar -->
            <div style="margin-bottom: 16px;">
                <div style="font-size: 8px; color: #666; margin-bottom: 6px; font-weight: 600;">ALLOCATION OVERVIEW</div>
                <table style="width: 100%; height: 24px; border-collapse: collapse; table-layout: fixed;">
                    <tr>
                        {allocation_bar_segments}
                    </tr>
                </table>
            </div>

            <table style="margin-bottom: 16px;">
                <thead>
                    <tr>
                        <th style="width: 20px;"></th>
                        <th>Asset Class</th>
                        <th class="number">Weight</th>
                        <th class="number">Value</th>
                        <th>Distribution</th>
                    </tr>
                </thead>
                <tbody>
                    {allocation_rows}
                </tbody>
            </table>

            {f'''
            <div class="section-header">
                <h2>Sub-Asset Breakdown</h2>
            </div>

            <table style="margin-bottom: 16px;">
                <thead>
                    <tr>
                        <th style="width: 20px;"></th>
                        <th>Category</th>
                        <th class="number">Weight</th>
                        <th class="number">Value</th>
                        <th>Distribution</th>
                    </tr>
                </thead>
                <tbody>
                    {sub_asset_rows}
                </tbody>
            </table>
            ''' if sub_asset_rows else ''}

        </div>

        <!-- Page 3: Allocation Details -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="commentary">
                Understanding your portfolio's sector and geographic exposure helps identify concentration risks and
                diversification opportunities. Sector allocation affects how your portfolio responds to economic cycles,
                while geographic exposure impacts currency risk and global market sensitivity.
            </div>

            <table class="two-col-table">
                <tr>
                    <td>
                        <div class="section-header" style="margin-top: 0;">
                            <h2>Sector Exposure</h2>
                        </div>
                        <!-- Sector Visual Bar -->
                        <table style="width: 100%; height: 18px; border-collapse: collapse; table-layout: fixed; margin-bottom: 10px;">
                            <tr>{sector_bar_segments}</tr>
                        </table>
                        <table>
                            <thead>
                                <tr>
                                    <th>Sector</th>
                                    <th>Distribution</th>
                                    <th class="number">Weight</th>
                                </tr>
                            </thead>
                            <tbody>
                                {sector_rows}
                            </tbody>
                        </table>
                    </td>
                    <td>
                        <div class="section-header" style="margin-top: 0;">
                            <h2>Geographic Distribution</h2>
                        </div>
                        <!-- Geography Visual Bar -->
                        <table style="width: 100%; height: 18px; border-collapse: collapse; table-layout: fixed; margin-bottom: 10px;">
                            <tr>{geo_bar_segments}</tr>
                        </table>
                        <table>
                            <thead>
                                <tr>
                                    <th>Region</th>
                                    <th>Distribution</th>
                                    <th class="number">Weight</th>
                                </tr>
                            </thead>
                            <tbody>
                                {geo_rows}
                            </tbody>
                        </table>
                    </td>
                </tr>
            </table>

            <div class="section-header">
                <h2>Sector Comparison vs S&amp;P 500</h2>
            </div>

            <div class="commentary">
                Comparing your sector weights to the S&amp;P 500 benchmark reveals where you are overweight or underweight
                relative to the broader market. Positive differences indicate sector bets that may enhance returns if
                those sectors outperform, while also adding tracking risk versus the benchmark.
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Sector</th>
                        <th style="text-align: center;">Portfolio</th>
                        <th class="number">%</th>
                        <th style="text-align: center;">S&amp;P 500</th>
                        <th class="number">%</th>
                        <th class="number">Diff</th>
                    </tr>
                </thead>
                <tbody>
                    {sector_compare_rows}
                </tbody>
            </table>

        </div>

        <!-- Page 4: Risk Analysis -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>Risk Analysis</h2>
            </div>

            <div class="commentary">
                Risk metrics quantify the potential for loss and volatility in your portfolio. Volatility measures
                return variability, Beta indicates market sensitivity, Sharpe Ratio shows risk-adjusted returns,
                and Maximum Drawdown reveals the worst historical decline. These metrics help determine if the portfolio
                matches your risk tolerance.
            </div>

            <table class="two-col-table" style="margin-bottom: 20px;">
                <tr>
                    <td>
                        <table class="metrics-table" style="margin-bottom: 0;">
                            <tr>
                                <td>
                                    <div class="metric-label">Annualized Volatility</div>
                                    <div class="metric-value">{fmt_pct(volatility) if volatility else 'N/A'}</div>
                                    <div class="metric-subtext">Standard deviation of returns</div>
                                </td>
                                <td>
                                    <div class="metric-label">Beta vs S&P 500</div>
                                    <div class="metric-value">{f'{beta:.2f}' if beta else 'N/A'}</div>
                                    <div class="metric-subtext">Market sensitivity</div>
                                </td>
                            </tr>
                            <tr>
                                <td>
                                    <div class="metric-label">Sharpe Ratio</div>
                                    <div class="metric-value">{f'{sharpe:.2f}' if sharpe else 'N/A'}</div>
                                    <div class="metric-subtext">Risk-adjusted return</div>
                                </td>
                                <td>
                                    <div class="metric-label">Max Drawdown</div>
                                    <div class="metric-value" style="color: {'#ff453a' if max_dd and max_dd < -10 else '#1a1a2e'};">{fmt_pct(max_dd) if max_dd else 'N/A'}</div>
                                    <div class="metric-subtext">Largest peak-to-trough decline</div>
                                </td>
                            </tr>
                        </table>
                    </td>
                    <td style="text-align: center; padding: 20px;">
                        <div style="font-size: 10px; color: #666; margin-bottom: 12px;">Risk Profile</div>
                        <div style="background: linear-gradient(to right, #30d158, #ffd60a, #ff9f0a, #ff453a); height: 20px; border-radius: 10px; position: relative; margin-bottom: 8px;">
                            <div style="position: absolute; left: {risk_pct}%; top: -3px; width: 6px; height: 26px; background: #1a1a2e; border-radius: 3px; border: 2px solid white; margin-left: -3px;"></div>
                        </div>
                        <table class="risk-labels-row" style="width: 100%;">
                            <tr>
                                <td style="text-align: left;">Conservative</td>
                                <td style="text-align: center;">Moderate</td>
                                <td style="text-align: right;">Aggressive</td>
                            </tr>
                        </table>
                        <div style="font-size: 12px; font-weight: 600; margin-top: 10px; color: #635bff;">{risk_label}</div>
                    </td>
                </tr>
            </table>

            <div class="section-header">
                <h2>Top 10 Holdings (Concentration Risk)</h2>
            </div>

            <div class="commentary">
                High concentration in individual positions increases idiosyncratic risk. If your top 10 holdings
                represent more than 50% of the portfolio, consider whether diversification could reduce volatility.
                Your top 10 positions account for {fmt_pct(concentration.get('top_10_weight'))} of the total portfolio value.
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 8%;">Rank</th>
                        <th>Symbol</th>
                        <th class="number">Value</th>
                        <th class="number">Weight</th>
                        <th>Concentration</th>
                    </tr>
                </thead>
                <tbody>
                    {top10_rows}
                </tbody>
            </table>

            <div class="section-header">
                <h2>Scenario Analysis</h2>
            </div>

            <div class="commentary">
                Scenario analysis shows how your portfolio might perform during market stress events. The estimated
                portfolio impact is calculated using your portfolio's beta. These hypothetical scenarios help you
                understand potential downside risks and prepare for market volatility.
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 40%;">Scenario</th>
                        <th class="number">Market Impact</th>
                        <th class="number">Est. Portfolio Impact</th>
                        <th class="number">Est. Portfolio Value</th>
                    </tr>
                </thead>
                <tbody>
                    {scenarios_rows}
                </tbody>
            </table>

        </div>

        <!-- Page 5: Projections -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>10-Year Projections (Monte Carlo Simulation)</h2>
            </div>

            <div class="commentary">
                Monte Carlo simulation uses random sampling to model {mc_simulations:,} possible future outcomes based on
                historical return patterns and your portfolio's volatility. The percentile ranges show the distribution
                of outcomes—the median represents the most likely scenario, while the 10th and 90th percentiles show
                pessimistic and optimistic cases respectively.
            </div>

            <table class="metrics-table" style="margin-bottom: 20px;">
                <tr>
                    <td>
                        <div class="metric-label">Simulations Run</div>
                        <div class="metric-value">{mc_simulations:,}</div>
                    </td>
                    <td>
                        <div class="metric-label">Starting Value</div>
                        <div class="metric-value">{fmt_currency(total_value)}</div>
                    </td>
                    <td>
                        <div class="metric-label">Probability of Gain</div>
                        <div class="metric-value" style="color: #30d158;">{fmt_pct(mc_prob_gain) if mc_prob_gain is not None else 'N/A'}</div>
                    </td>
                    <td>
                        <div class="metric-label">Chance to Double</div>
                        <div class="metric-value">{fmt_pct(mc_chance_double) if mc_chance_double is not None else 'N/A'}</div>
                    </td>
                </tr>
            </table>

            {f'''
            <table class="metrics-table" style="margin-bottom: 20px;">
                <tr>
                    <td>
                        <div class="metric-label">Implied CAGR (Median)</div>
                        <div class="metric-value" style="color: #635bff;">{fmt_pct(mc_implied_cagr) if mc_implied_cagr is not None else 'N/A'}</div>
                        <div class="metric-subtext">Compound Annual Growth Rate</div>
                    </td>
                    <td>
                        <div class="metric-label">10-Year Median Value</div>
                        <div class="metric-value">{fmt_currency(mc_summary.get("year_10", {}).get("median"))}</div>
                        <div class="metric-subtext">50th percentile outcome</div>
                    </td>
                    <td>
                        <div class="metric-label">10-Year Best Case</div>
                        <div class="metric-value" style="color: #30d158;">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_90"))}</div>
                        <div class="metric-subtext">90th percentile outcome</div>
                    </td>
                    <td>
                        <div class="metric-label">10-Year Worst Case</div>
                        <div class="metric-value" style="color: #ff453a;">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_10"))}</div>
                        <div class="metric-subtext">10th percentile outcome</div>
                    </td>
                </tr>
            </table>
            ''' if mc_summary else ''}

            <div class="section-header">
                <h2>Projected Values by Year</h2>
            </div>

            {f'''
            <table>
                <thead>
                    <tr>
                        <th>Time Horizon</th>
                        <th class="number">5th Percentile (Worst)</th>
                        <th class="number">25th Percentile</th>
                        <th class="number">50th Percentile (Median)</th>
                        <th class="number">75th Percentile</th>
                        <th class="number">95th Percentile (Best)</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Year 1</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_1", {}).get("percentile_5"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_1", {}).get("percentile_25"))}</td>
                        <td class="number" style="font-weight: 600;">{fmt_currency(mc_summary.get("year_1", {}).get("median"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_1", {}).get("percentile_75"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_1", {}).get("percentile_95"))}</td>
                    </tr>
                    <tr style="background: #fafafa;">
                        <td>Year 5</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_5", {}).get("percentile_5"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_5", {}).get("percentile_25"))}</td>
                        <td class="number" style="font-weight: 600;">{fmt_currency(mc_summary.get("year_5", {}).get("median"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_5", {}).get("percentile_75"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_5", {}).get("percentile_95"))}</td>
                    </tr>
                    <tr>
                        <td>Year 10</td>
                        <td class="number" style="color: #ff453a;">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_5"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_25"))}</td>
                        <td class="number" style="font-weight: 600;">{fmt_currency(mc_summary.get("year_10", {}).get("median"))}</td>
                        <td class="number">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_75"))}</td>
                        <td class="number" style="color: #30d158;">{fmt_currency(mc_summary.get("year_10", {}).get("percentile_95"))}</td>
                    </tr>
                </tbody>
            </table>
            ''' if mc_summary else '<p style="color: #666; font-size: 9px;">Monte Carlo projections not available for this portfolio.</p>'}

        </div>

        <!-- Page 6: Key Insights -->
        {f'''
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>Key Insights &amp; Recommendations</h2>
            </div>

            <div class="commentary">
                Based on the analysis of your portfolio's allocation, risk metrics, and market exposures, the following
                insights highlight areas of strength and potential opportunities for optimization. These actionable
                recommendations can help align your portfolio with your investment objectives.
            </div>

            <table style="width: 100%;">
                {insights_rows}
            </table>

        </div>
        ''' if insights_rows else ''}

        <!-- Page 7+: Holdings Detail -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>Holdings Detail</h2>
            </div>

            <div class="commentary">
                Complete list of all {len(positions)} positions in your portfolio, sorted by market value. The weight column
                shows each holding's percentage of the total portfolio. Review this list periodically to ensure
                individual position sizes remain appropriate for your risk tolerance and investment strategy.
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 32%;">Security</th>
                        <th class="number">Shares</th>
                        <th class="number">Price</th>
                        <th class="number">Market Value</th>
                        <th class="number" style="width: 14%;">Weight</th>
                    </tr>
                </thead>
                <tbody>
                    {positions_rows}
                </tbody>
            </table>

        </div>

        <!-- Final Page: Glossary & Disclosures -->
        <div class="content-page">
            <div class="page-header">
                <span class="page-header-logo"></span>
                <span class="page-header-brand">Statement Scan</span>
                <span class="page-header-title">Portfolio Analysis Report</span>
            </div>

            <div class="section-header">
                <h2>Glossary of Terms</h2>
            </div>

            <div class="commentary">
                This glossary defines key financial terms used throughout this report to help you better understand
                the metrics and analysis presented.
            </div>

            <table class="glossary-table">
                <tr>
                    <td>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Volatility</div>
                            <div class="glossary-term-def">A statistical measure of the dispersion of returns, commonly measured as the annualized standard deviation of returns. Higher volatility indicates greater price fluctuation.</div>
                        </div>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Beta</div>
                            <div class="glossary-term-def">A measure of systematic risk relative to the market (S&P 500). A beta of 1.0 indicates market-like volatility; greater than 1.0 means more volatile than the market.</div>
                        </div>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Sharpe Ratio</div>
                            <div class="glossary-term-def">Risk-adjusted return metric calculated as (Portfolio Return - Risk-Free Rate) / Portfolio Volatility. Higher values indicate better risk-adjusted performance.</div>
                        </div>
                    </td>
                    <td>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Maximum Drawdown</div>
                            <div class="glossary-term-def">The largest peak-to-trough decline in portfolio value over a specific period. Measures the worst-case loss scenario from a portfolio's highest point.</div>
                        </div>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Asset Allocation</div>
                            <div class="glossary-term-def">The distribution of investments across major asset classes such as stocks, bonds, cash, and alternatives. A key driver of long-term portfolio risk and return.</div>
                        </div>
                        <div class="glossary-term">
                            <div class="glossary-term-title">Concentration Risk</div>
                            <div class="glossary-term-def">The risk of loss arising from having a large portion of the portfolio invested in a single security or small group of securities.</div>
                        </div>
                    </td>
                </tr>
            </table>

            <div class="disclosures">
                <h3>Important Disclosures</h3>
                <p>This report is generated by Statement Scan for informational purposes only and does not constitute investment advice, a recommendation, or an offer to buy or sell any securities. The information contained herein is based on data provided by the user and publicly available market data.</p>
                <p>Past performance is not indicative of future results. The projections and scenario analyses presented are hypothetical and based on historical data and statistical models. Actual results may differ materially from those projected.</p>
                <p>Risk metrics are calculated using historical data and standard financial models. These metrics have limitations and may not fully capture all aspects of portfolio risk. Investors should consider their individual circumstances, risk tolerance, and investment objectives before making investment decisions.</p>
                <p>Statement Scan does not guarantee the accuracy, completeness, or timeliness of the information provided. Users should verify all information independently and consult with a qualified financial advisor before making investment decisions.</p>
            </div>

        </div>
    </body>
    </html>
    """

    return html


@app.route('/report/pdf', methods=['POST'])
def generate_pdf_report():
    """Generate a PDF report of the portfolio analysis."""
    if not WEASYPRINT_AVAILABLE:
        return jsonify({'error': 'PDF generation is not available'}), 503

    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Generate report date
        report_date = datetime.now().strftime('%B %d, %Y')

        # Generate HTML
        html_content = generate_report_html(data, report_date)

        # Convert to PDF
        pdf_bytes = HTML(string=html_content).write_pdf()

        # Return PDF as response
        response = Response(pdf_bytes, mimetype='application/pdf')
        response.headers['Content-Disposition'] = f'attachment; filename="portfolio-report-{datetime.now().strftime("%Y%m%d")}.pdf"'

        return response

    except Exception as e:
        return jsonify({'error': f'Failed to generate PDF: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
