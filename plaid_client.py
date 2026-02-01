"""
Plaid API client wrapper for Statement Scan Enterprise.
Handles account linking, token exchange, and holdings retrieval.
"""

import os
from datetime import datetime
from cryptography.fernet import Fernet

# Try to import plaid
try:
    import plaid
    from plaid.api import plaid_api
    from plaid.model.link_token_create_request import LinkTokenCreateRequest
    from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
    from plaid.model.accounts_get_request import AccountsGetRequest
    from plaid.model.item_remove_request import ItemRemoveRequest
    from plaid.model.products import Products
    from plaid.model.country_code import CountryCode
    PLAID_AVAILABLE = True
except ImportError:
    PLAID_AVAILABLE = False


class PlaidClient:
    """Wrapper for Plaid API operations."""

    def __init__(self):
        self.client_id = os.environ.get('PLAID_CLIENT_ID')
        self.secret = os.environ.get('PLAID_SECRET')
        self.env = os.environ.get('PLAID_ENV', 'sandbox')

        # Encryption key for access tokens
        self.encryption_key = os.environ.get('PLAID_ENCRYPTION_KEY')
        if self.encryption_key:
            self.fernet = Fernet(self.encryption_key.encode())
        else:
            self.fernet = None

        self.client = None
        if PLAID_AVAILABLE and self.client_id and self.secret:
            self._init_client()

    def _init_client(self):
        """Initialize Plaid API client."""
        if self.env == 'sandbox':
            host = plaid.Environment.Sandbox
        elif self.env == 'development':
            host = plaid.Environment.Development
        else:
            host = plaid.Environment.Production

        configuration = plaid.Configuration(
            host=host,
            api_key={
                'clientId': self.client_id,
                'secret': self.secret,
            }
        )
        api_client = plaid.ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

    def is_configured(self):
        """Check if Plaid is properly configured."""
        return PLAID_AVAILABLE and self.client is not None

    def encrypt_token(self, token):
        """Encrypt an access token for storage."""
        if self.fernet:
            return self.fernet.encrypt(token.encode()).decode()
        # Fallback: store unencrypted (not recommended for production)
        return token

    def decrypt_token(self, encrypted_token):
        """Decrypt a stored access token."""
        if self.fernet:
            return self.fernet.decrypt(encrypted_token.encode()).decode()
        return encrypted_token

    def create_link_token(self, user_id, redirect_uri=None):
        """Create a Link token for initializing Plaid Link."""
        if not self.is_configured():
            raise ValueError('Plaid is not configured')

        try:
            # Build request parameters
            request_params = {
                'user': LinkTokenCreateRequestUser(client_user_id=str(user_id)),
                'client_name': 'Statement Scan',
                'products': [Products('investments')],
                'country_codes': [CountryCode('US')],
                'language': 'en'
            }

            # Only include redirect_uri if provided
            if redirect_uri:
                request_params['redirect_uri'] = redirect_uri

            request = LinkTokenCreateRequest(**request_params)

            response = self.client.link_token_create(request)
            return response.to_dict()
        except plaid.ApiException as e:
            # Extract the actual error message from Plaid
            error_body = e.body if hasattr(e, 'body') else str(e)
            raise ValueError(f'Plaid API error: {error_body}')

    def exchange_public_token(self, public_token):
        """Exchange a public token for an access token."""
        if not self.is_configured():
            raise ValueError('Plaid is not configured')

        request = ItemPublicTokenExchangeRequest(public_token=public_token)
        response = self.client.item_public_token_exchange(request)

        return {
            'access_token': response.access_token,
            'item_id': response.item_id
        }

    def get_accounts(self, access_token):
        """Get accounts for a connected item."""
        if not self.is_configured():
            raise ValueError('Plaid is not configured')

        request = AccountsGetRequest(access_token=access_token)
        response = self.client.accounts_get(request)

        return {
            'accounts': [acc.to_dict() for acc in response.accounts],
            'item': response.item.to_dict()
        }

    def get_holdings(self, access_token):
        """Get investment holdings for a connected item."""
        if not self.is_configured():
            raise ValueError('Plaid is not configured')

        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.client.investments_holdings_get(request)

        return {
            'holdings': [h.to_dict() for h in response.holdings],
            'securities': [s.to_dict() for s in response.securities],
            'accounts': [a.to_dict() for a in response.accounts]
        }

    def remove_item(self, access_token):
        """Remove an item (disconnect account)."""
        if not self.is_configured():
            raise ValueError('Plaid is not configured')

        request = ItemRemoveRequest(access_token=access_token)
        response = self.client.item_remove(request)
        return response.to_dict()

    def holdings_to_positions(self, holdings_response):
        """Convert Plaid holdings to Statement Scan position format."""
        positions = []

        # Create a lookup for securities
        securities = {s['security_id']: s for s in holdings_response.get('securities', [])}

        for holding in holdings_response.get('holdings', []):
            security_id = holding.get('security_id')
            security = securities.get(security_id, {})

            symbol = security.get('ticker_symbol', '')
            if not symbol:
                # Skip holdings without a symbol
                continue

            position = {
                'symbol': symbol.upper(),
                'description': security.get('name', ''),
                'shares': holding.get('quantity', 0),
                'price': holding.get('institution_price', 0),
                'value': holding.get('institution_value', 0)
            }

            # Recalculate value if not provided
            if not position['value'] and position['shares'] and position['price']:
                position['value'] = position['shares'] * position['price']

            positions.append(position)

        return positions


# Global client instance
plaid_client = PlaidClient()
