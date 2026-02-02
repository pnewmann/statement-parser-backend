"""
Database models for Statement Scan Enterprise.
Includes User, Portfolio, and PlaidConnection models.
"""

from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class User(db.Model):
    """User account model."""
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Password reset fields
    reset_token = db.Column(db.String(100), unique=True, nullable=True, index=True)
    reset_token_expires = db.Column(db.DateTime, nullable=True)

    # Relationships
    portfolios = db.relationship('Portfolio', backref='user', lazy=True, cascade='all, delete-orphan')
    plaid_connections = db.relationship('PlaidConnection', backref='user', lazy=True, cascade='all, delete-orphan')

    def to_dict(self):
        """Convert user to dictionary (excluding password)."""
        return {
            'id': self.id,
            'email': self.email,
            'name': self.name,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class Portfolio(db.Model):
    """Saved portfolio model."""
    __tablename__ = 'portfolios'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    positions = db.Column(db.JSON, nullable=False)  # Array of position objects
    total_value = db.Column(db.Numeric(15, 2))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert portfolio to dictionary."""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'name': self.name,
            'description': self.description,
            'positions': self.positions,
            'total_value': float(self.total_value) if self.total_value else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class PlaidConnection(db.Model):
    """Plaid account connection model."""
    __tablename__ = 'plaid_connections'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    item_id = db.Column(db.String(255), nullable=False)
    access_token_encrypted = db.Column(db.Text, nullable=False)
    institution_name = db.Column(db.String(255))
    institution_id = db.Column(db.String(255))
    last_synced = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        """Convert connection to dictionary (excluding access token)."""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'item_id': self.item_id,
            'institution_name': self.institution_name,
            'institution_id': self.institution_id,
            'last_synced': self.last_synced.isoformat() if self.last_synced else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
