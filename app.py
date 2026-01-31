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

# Money pattern: $1,234.56 or 1,234.56
MONEY_PATTERN = re.compile(r'\$?([\d,]+\.?\d*)')

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
    'A', 'I', 'X', 'Z',  # Single letters that appear in statements
    'HELD', 'THAT', 'THIS', 'WITH', 'FROM', 'HAVE', 'BEEN', 'EACH', 'WILL',
    'MORE', 'WHEN', 'THEM', 'BEEN', 'CALL', 'FIRST', 'WATER', 'THAN', 'LONG',
    'EL', 'TX', 'CA', 'NY', 'FL', 'CO', 'AZ', 'NC', 'VA', 'WA', 'MA', 'PA',  # State abbrevs
}

# Known ETF/Stock symbols to definitely include
KNOWN_SYMBOLS = {
    'SGOV', 'AGG', 'BND', 'BNDX', 'VTIP', 'STIP', 'TIP', 'TIPS', 'SCHZ', 'SCHP',
    'VTI', 'VOO', 'SPY', 'QQQ', 'IVV', 'VEA', 'VWO', 'IEFA', 'IEMG', 'VIG',
    'SCHD', 'SCHA', 'SCHB', 'SCHF', 'SCHE', 'SCHX', 'SCHY', 'SCHG', 'SCHV',
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK',
    'JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'VZ',
    'ADBE', 'NFLX', 'CRM', 'PFE', 'TMO', 'COST', 'PEP', 'AVGO', 'CSCO', 'ACN',
}


def clean_number(value):
    """Convert string number to float, handling commas and dollar signs."""
    if not value:
        return None
    cleaned = re.sub(r'[$,]', '', str(value).strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def is_valid_symbol(text):
    """Check if text looks like a stock symbol."""
    if not text:
        return False
    text = text.strip().upper()

    # Check exclusion list first
    if text in EXCLUDED_WORDS:
        return False

    # Known symbols are always valid
    if text in KNOWN_SYMBOLS:
        return True

    # Must be 1-5 uppercase letters
    if not SYMBOL_PATTERN.match(text):
        # Also allow some special cases like BRK.B, BF.A
        if not re.match(r'^[A-Z]{1,4}\.[A-Z]$', text):
            return False

    # Additional heuristics to filter out non-symbols
    # If it's a common English word pattern, reject it
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
    """Parse Charles Schwab brokerage statement."""
    positions = []
    cash_positions = []

    for page in pdf.pages:
        text = page.extract_text() or ""

        # Check if this page has the positions table
        if 'Exchange Traded Funds' in text or 'Positions' in text:
            tables = page.extract_tables()

            for table in tables:
                if not table:
                    continue

                # Look for the ETF positions table
                # Schwab format: Symbol, Description, Quantity, Price($), Market Value($), ...
                for row in table:
                    if not row or len(row) < 4:
                        continue

                    # Clean the row
                    row = [str(cell).strip() if cell else '' for cell in row]

                    # Skip header rows
                    row_text = ' '.join(row).lower()
                    if 'symbol' in row_text and 'description' in row_text:
                        continue
                    if 'total' in row_text.lower() and 'exchange' in row_text.lower():
                        continue

                    # Find a valid symbol in the row
                    symbol = None
                    symbol_idx = None

                    for i, cell in enumerate(row):
                        cell_clean = cell.strip().upper()
                        # Remove any special characters like ◊
                        cell_clean = re.sub(r'[^\w.]', '', cell_clean)

                        if cell_clean and is_valid_symbol(cell_clean):
                            symbol = cell_clean
                            symbol_idx = i
                            break

                    if not symbol:
                        continue

                    # Extract description (usually right after symbol)
                    description = ''
                    if symbol_idx is not None and symbol_idx + 1 < len(row):
                        desc = row[symbol_idx + 1].strip()
                        # Clean up description - remove special chars
                        desc = re.sub(r'[◊,]', '', desc).strip()
                        if desc and len(desc) > 2 and not is_valid_symbol(desc):
                            description = desc

                    # Extract numbers from the row
                    numbers = []
                    for cell in row:
                        num = clean_number(cell)
                        if num is not None and num > 0:
                            numbers.append(num)

                    if len(numbers) >= 2:
                        # Sort to identify: typically quantity < price < value
                        # But we need to be smarter - look for patterns

                        # Find quantity (usually has 4 decimal places in Schwab)
                        quantity = None
                        price = None
                        value = None

                        for num in numbers:
                            num_str = str(num)
                            # Quantity often has many decimal places
                            if '.' in num_str:
                                decimals = len(num_str.split('.')[1])
                                if decimals >= 4 and num < 100000:
                                    quantity = num
                                    continue
                            # Price is usually between 1 and 1000
                            if 1 < num < 1000 and price is None:
                                price = num
                            # Value is usually the largest number
                            if num > 100:
                                if value is None or num > value:
                                    value = num

                        # Fallback: use position in sorted list
                        if quantity is None and len(numbers) >= 1:
                            numbers_sorted = sorted(numbers)
                            # Smallest is often quantity (unless it's a price)
                            if numbers_sorted[0] < 10000:
                                quantity = numbers_sorted[0]

                        if value is None and len(numbers) >= 1:
                            value = max(numbers)

                        # Calculate price if we have quantity and value
                        if quantity and value and quantity > 0 and price is None:
                            calc_price = value / quantity
                            if 0.01 < calc_price < 10000:
                                price = round(calc_price, 2)

                        position = {
                            'symbol': symbol,
                            'description': description,
                            'shares': round(quantity, 4) if quantity else None,
                            'price': round(price, 2) if price else None,
                            'value': round(value, 2) if value else None
                        }

                        # Only add if we have meaningful data
                        if position['value'] and position['value'] > 10:
                            positions.append(position)

        # Also look for cash positions
        if 'Cash and Cash Investments' in text or 'Bank Sweep' in text:
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                for row in table:
                    if not row:
                        continue
                    row_text = ' '.join(str(c) for c in row if c).lower()

                    # Look for bank sweep ending balance
                    if 'charles schwab bank' in row_text or 'bank sweep' in row_text:
                        if 'total' in row_text:
                            continue
                        numbers = []
                        for cell in row:
                            num = clean_number(cell)
                            if num and num > 100:
                                numbers.append(num)

                        if numbers:
                            # Look for ending balance (usually last significant number)
                            for num in reversed(numbers):
                                if num > 0:
                                    cash_positions.append({
                                        'symbol': 'CASH',
                                        'description': 'Charles Schwab Bank Sweep',
                                        'shares': None,
                                        'price': None,
                                        'value': round(num, 2)
                                    })
                                    break

    # Combine positions, removing duplicates
    seen = set()
    unique_positions = []

    for p in positions:
        if p['symbol'] not in seen:
            seen.add(p['symbol'])
            unique_positions.append(p)

    # Add cash if found and not duplicate
    for c in cash_positions:
        if c['symbol'] not in seen:
            seen.add(c['symbol'])
            unique_positions.append(c)
            break  # Only add one cash position

    return unique_positions


def parse_fidelity_pdf(pdf):
    """Parse Fidelity brokerage statement."""
    positions = []

    for page in pdf.pages:
        text = page.extract_text() or ""
        tables = page.extract_tables()

        for table in tables:
            if not table:
                continue
            for row in table:
                if not row or len(row) < 3:
                    continue

                # Clean row
                row = [str(cell).strip() if cell else '' for cell in row]

                # Find symbol
                symbol = None
                symbol_idx = None

                for i, cell in enumerate(row):
                    cell_clean = cell.strip().upper()
                    if is_valid_symbol(cell_clean):
                        symbol = cell_clean
                        symbol_idx = i
                        break

                if not symbol:
                    continue

                # Get description
                description = ''
                if symbol_idx is not None and symbol_idx + 1 < len(row):
                    desc = row[symbol_idx + 1].strip()
                    if desc and len(desc) > 2 and not is_valid_symbol(desc):
                        description = desc

                # Get numbers
                numbers = []
                for cell in row:
                    num = clean_number(cell)
                    if num is not None and num > 0:
                        numbers.append(num)

                if len(numbers) >= 2:
                    numbers.sort()
                    position = {
                        'symbol': symbol,
                        'description': description,
                        'shares': numbers[0] if numbers[0] < 100000 else None,
                        'price': None,
                        'value': numbers[-1] if numbers[-1] > 1 else None
                    }

                    if position['shares'] and position['value']:
                        position['price'] = round(position['value'] / position['shares'], 2)

                    if position['value'] and position['value'] > 10:
                        positions.append(position)

    # Remove duplicates
    seen = set()
    unique = []
    for p in positions:
        if p['symbol'] not in seen:
            seen.add(p['symbol'])
            unique.append(p)

    return unique


def parse_csv_file(content):
    """Parse a CSV file from various brokerages."""
    positions = []

    # Try to decode if bytes
    if isinstance(content, bytes):
        content = content.decode('utf-8-sig')  # Handle BOM

    # Read CSV
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)

    if not rows:
        return positions

    # Find header row
    header_row = None
    header_index = 0

    for i, row in enumerate(rows):
        row_lower = [str(cell).lower() for cell in row]
        # Look for common column headers
        if any(h in row_lower for h in ['symbol', 'ticker', 'security']):
            header_row = row
            header_index = i
            break

    if not header_row:
        # Try first row as header
        header_row = rows[0]
        header_index = 0

    # Map column indices
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

    # Parse data rows
    for row in rows[header_index + 1:]:
        if len(row) <= max(filter(None, [symbol_idx, desc_idx, shares_idx, price_idx, value_idx]), default=0):
            continue

        symbol = None
        if symbol_idx is not None and symbol_idx < len(row):
            symbol = str(row[symbol_idx]).strip().upper()

        if not symbol or not is_valid_symbol(symbol):
            # Try to find symbol in any column
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

        # Only add if we have at least symbol and one other field
        if position['shares'] or position['value']:
            positions.append(position)

    return positions


def parse_pdf_file(content):
    """Parse a PDF brokerage statement."""
    positions = []

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        # Get full text to detect brokerage
        full_text = ""
        for page in pdf.pages[:3]:  # Check first 3 pages
            full_text += (page.extract_text() or "") + "\n"

        brokerage = detect_brokerage_pdf(full_text)

        # Parse based on brokerage
        if brokerage == 'schwab':
            positions = parse_schwab_pdf(pdf)
        elif brokerage == 'fidelity':
            positions = parse_fidelity_pdf(pdf)
        else:
            # Generic parsing - be more conservative
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    for row in table:
                        if not row or len(row) < 3:
                            continue

                        row = [str(cell).strip() if cell else '' for cell in row]

                        for i, cell in enumerate(row):
                            if cell and is_valid_symbol(cell.upper()):
                                # Get numbers
                                numbers = []
                                for c in row:
                                    num = clean_number(c)
                                    if num and num > 0:
                                        numbers.append(num)

                                if len(numbers) >= 2:
                                    numbers.sort()
                                    position = {
                                        'symbol': cell.upper(),
                                        'description': '',
                                        'shares': numbers[0] if numbers[0] < 100000 else None,
                                        'price': None,
                                        'value': numbers[-1] if numbers[-1] > 1 else None
                                    }
                                    if position['value'] and position['value'] > 10:
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
