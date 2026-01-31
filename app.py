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

# Shares pattern: numbers with optional decimals
SHARES_PATTERN = re.compile(r'^[\d,]+\.?\d*$')


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
    # Common valid symbols are 1-5 uppercase letters
    if SYMBOL_PATTERN.match(text):
        return True
    # Also allow some special cases like BRK.B, BF.A
    if re.match(r'^[A-Z]{1,4}\.[A-Z]$', text):
        return True
    return False


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
    full_text = ""

    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

        # Try to extract tables
        tables = page.extract_tables()
        for table in tables:
            if not table:
                continue
            for row in table:
                if not row or len(row) < 3:
                    continue
                # Look for rows with stock symbols
                for i, cell in enumerate(row):
                    if cell and is_valid_symbol(cell):
                        position = extract_position_from_row(row, i)
                        if position:
                            positions.append(position)
                            break

    # If table extraction didn't work well, try text parsing
    if len(positions) < 2:
        positions = parse_positions_from_text(full_text, 'schwab')

    return positions


def parse_fidelity_pdf(pdf):
    """Parse Fidelity brokerage statement."""
    positions = []
    full_text = ""

    for page in pdf.pages:
        text = page.extract_text() or ""
        full_text += text + "\n"

        tables = page.extract_tables()
        for table in tables:
            if not table:
                continue
            for row in table:
                if not row or len(row) < 3:
                    continue
                for i, cell in enumerate(row):
                    if cell and is_valid_symbol(cell):
                        position = extract_position_from_row(row, i)
                        if position:
                            positions.append(position)
                            break

    if len(positions) < 2:
        positions = parse_positions_from_text(full_text, 'fidelity')

    return positions


def extract_position_from_row(row, symbol_index):
    """Extract position data from a table row."""
    try:
        symbol = row[symbol_index].strip().upper()

        # Look for description (usually before or after symbol)
        description = ""
        if symbol_index > 0 and row[symbol_index - 1]:
            desc_candidate = str(row[symbol_index - 1]).strip()
            if len(desc_candidate) > 3 and not is_valid_symbol(desc_candidate):
                description = desc_candidate
        if not description and symbol_index < len(row) - 1 and row[symbol_index + 1]:
            desc_candidate = str(row[symbol_index + 1]).strip()
            if len(desc_candidate) > 3 and not is_valid_symbol(desc_candidate):
                description = desc_candidate

        # Look for numbers in the rest of the row
        numbers = []
        for cell in row:
            if cell:
                num = clean_number(cell)
                if num is not None and num > 0:
                    numbers.append(num)

        if len(numbers) >= 2:
            # Heuristic: smaller number is likely shares, larger is value
            numbers.sort()
            shares = numbers[0] if numbers[0] < 100000 else None
            value = numbers[-1] if numbers[-1] > 1 else None

            # Try to find price (value / shares)
            price = None
            if shares and value and shares > 0:
                potential_price = value / shares
                if 0.01 < potential_price < 100000:
                    price = round(potential_price, 2)

            return {
                'symbol': symbol,
                'description': description,
                'shares': shares,
                'price': price,
                'value': value
            }
    except Exception:
        pass
    return None


def parse_positions_from_text(text, brokerage):
    """Parse positions from raw text when table extraction fails."""
    positions = []
    lines = text.split('\n')

    for i, line in enumerate(lines):
        words = line.split()
        for j, word in enumerate(words):
            if is_valid_symbol(word):
                # Try to extract numbers from this line and surrounding lines
                context = ' '.join(lines[max(0, i-1):min(len(lines), i+2)])
                numbers = []
                for match in MONEY_PATTERN.finditer(context):
                    num = clean_number(match.group(0))
                    if num and num > 0:
                        numbers.append(num)

                if len(numbers) >= 2:
                    numbers.sort()
                    position = {
                        'symbol': word.upper(),
                        'description': '',
                        'shares': numbers[0] if numbers[0] < 100000 else None,
                        'price': None,
                        'value': numbers[-1] if numbers[-1] > 1 else None
                    }

                    # Avoid duplicates
                    if not any(p['symbol'] == position['symbol'] for p in positions):
                        positions.append(position)
                break

    return positions


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
            # Generic parsing
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    for row in table:
                        if not row or len(row) < 2:
                            continue
                        for i, cell in enumerate(row):
                            if cell and is_valid_symbol(str(cell)):
                                position = extract_position_from_row(row, i)
                                if position:
                                    positions.append(position)
                                    break

            if len(positions) < 2:
                positions = parse_positions_from_text(full_text, brokerage)

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
