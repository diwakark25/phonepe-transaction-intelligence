import sqlite3
import os
from typing import Optional

class DatabaseManager:
    def __init__(self, db_path: str = "phonepe_insights.db"):
        """Initialize database manager with SQLite connection"""
        self.db_path = db_path
        self.connection = None
        
    def connect(self) -> sqlite3.Connection:
        """Establish database connection"""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            return self.connection
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def create_tables(self):
        """Create all required tables for PhonePe analytics"""
        
        create_table_queries = [
            # Aggregated Transaction Table
            """
            CREATE TABLE IF NOT EXISTS aggregated_transaction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                transaction_type VARCHAR(50) NOT NULL,
                transaction_count INTEGER NOT NULL DEFAULT 0,
                transaction_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Aggregated User Table
            """
            CREATE TABLE IF NOT EXISTS aggregated_user (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                brands VARCHAR(100) NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Aggregated Insurance Table
            """
            CREATE TABLE IF NOT EXISTS aggregated_insurance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                insurance_type VARCHAR(50) NOT NULL,
                insurance_count INTEGER NOT NULL DEFAULT 0,
                insurance_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Map Transaction Table
            """
            CREATE TABLE IF NOT EXISTS map_transaction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                district VARCHAR(100) NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Map User Table
            """
            CREATE TABLE IF NOT EXISTS map_user (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                district VARCHAR(100) NOT NULL,
                registered_users INTEGER NOT NULL DEFAULT 0,
                app_opens INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Map Insurance Table
            """
            CREATE TABLE IF NOT EXISTS map_insurance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                district VARCHAR(100) NOT NULL,
                insurance_count INTEGER NOT NULL DEFAULT 0,
                insurance_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Top Transaction Table
            """
            CREATE TABLE IF NOT EXISTS top_transaction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                pincode VARCHAR(10) NOT NULL,
                transaction_count INTEGER NOT NULL DEFAULT 0,
                transaction_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Top User Table
            """
            CREATE TABLE IF NOT EXISTS top_user (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                pincode VARCHAR(10) NOT NULL,
                registered_users INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Top Insurance Table
            """
            CREATE TABLE IF NOT EXISTS top_insurance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                pincode VARCHAR(10) NOT NULL,
                insurance_count INTEGER NOT NULL DEFAULT 0,
                insurance_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        # Create indexes separately (SQLite compatible way)
        create_index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_state_year_quarter ON aggregated_transaction(state, year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_transaction_type ON aggregated_transaction(transaction_type)",
            "CREATE INDEX IF NOT EXISTS idx_state_year_quarter_user ON aggregated_user(state, year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_brands ON aggregated_user(brands)",
            "CREATE INDEX IF NOT EXISTS idx_state_year_quarter_insurance ON aggregated_insurance(state, year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_insurance_type ON aggregated_insurance(insurance_type)",
            "CREATE INDEX IF NOT EXISTS idx_state_district ON map_transaction(state, district)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_map ON map_transaction(year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_state_district_user ON map_user(state, district)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_map_user ON map_user(year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_state_district_insurance ON map_insurance(state, district)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_map_insurance ON map_insurance(year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_state_pincode ON top_transaction(state, pincode)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_top ON top_transaction(year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_state_pincode_user ON top_user(state, pincode)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_top_user ON top_user(year, quarter)",
            "CREATE INDEX IF NOT EXISTS idx_state_pincode_insurance ON top_insurance(state, pincode)",
            "CREATE INDEX IF NOT EXISTS idx_year_quarter_top_insurance ON top_insurance(year, quarter)"
        ]
        
        try:
            cursor = self.connection.cursor()
            
            # Create tables first
            for query in create_table_queries:
                cursor.execute(query)
            
            # Create indexes separately
            for query in create_index_queries:
                cursor.execute(query)
                
            self.connection.commit()
            print("✅ All tables and indexes created successfully!")
            
        except sqlite3.Error as e:
            print(f"❌ Error creating tables: {e}")
            raise
