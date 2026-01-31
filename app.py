"""
Brokerage Statement Parser API
Extracts positions from Schwab, Fidelity, and other brokerage statements.
"""

import io
import csv
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
import pdfplumber

app = Flask(__name__)
CORS(app)

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
                            # Add spaces to camelCase (ISHARESCOREUS -> ISHARES CORE US)
                            description = re.sub(r'([a-z])([A-Z])', r'\1 \2', description)
                            description = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', description)
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
