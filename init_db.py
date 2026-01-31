#!/usr/bin/env python3
"""
Database initialization script for Statement Scan Enterprise.
Run this script to create all database tables.

Usage:
    python init_db.py

Make sure DATABASE_URL environment variable is set.
"""

import os
import sys

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from models import db

def create_app():
    """Create Flask app with database configuration."""
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

    db.init_app(app)

    return app

def init_database():
    """Initialize the database by creating all tables."""
    app = create_app()

    with app.app_context():
        print(f"Connecting to database...")
        print(f"Database URI: {app.config['SQLALCHEMY_DATABASE_URI'][:50]}...")

        # Create all tables
        db.create_all()

        print("Database tables created successfully!")
        print("\nTables created:")
        print("  - users")
        print("  - portfolios")
        print("  - plaid_connections")

if __name__ == '__main__':
    init_database()
