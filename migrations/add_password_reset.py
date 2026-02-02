#!/usr/bin/env python3
"""
Migration: Add password reset columns to users table.

Run this script once to add the reset_token and reset_token_expires columns.
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask
from models import db

def create_app():
    """Create Flask app with database configuration."""
    app = Flask(__name__)

    database_url = os.environ.get('DATABASE_URL', 'sqlite:///statement_scan.db')

    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql+psycopg://', 1)
    elif database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', 'postgresql+psycopg://', 1)

    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    return app

def migrate():
    """Add password reset columns to users table."""
    app = create_app()

    with app.app_context():
        print("Running migration: Add password reset columns...")

        # Check if using SQLite or PostgreSQL
        is_sqlite = 'sqlite' in app.config['SQLALCHEMY_DATABASE_URI']

        try:
            if is_sqlite:
                # SQLite syntax
                db.session.execute(db.text('''
                    ALTER TABLE users ADD COLUMN reset_token VARCHAR(100) UNIQUE
                '''))
                db.session.execute(db.text('''
                    ALTER TABLE users ADD COLUMN reset_token_expires TIMESTAMP
                '''))
            else:
                # PostgreSQL syntax
                db.session.execute(db.text('''
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS reset_token VARCHAR(100) UNIQUE,
                    ADD COLUMN IF NOT EXISTS reset_token_expires TIMESTAMP
                '''))

            db.session.commit()
            print("Migration completed successfully!")
            print("Added columns: reset_token, reset_token_expires")

        except Exception as e:
            if 'duplicate column' in str(e).lower() or 'already exists' in str(e).lower():
                print("Columns already exist. Migration skipped.")
            else:
                print(f"Migration error: {e}")
                db.session.rollback()
                raise

if __name__ == '__main__':
    migrate()
