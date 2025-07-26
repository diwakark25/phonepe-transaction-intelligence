import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
from database.schema import DatabaseManager

class DatabaseOperations(DatabaseManager):
    def __init__(self, db_path: str = "phonepe_insights.db"):
        super().__init__(db_path)
        
    def insert_bulk_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Insert bulk data into specified table"""
        if not data:
            return False
            
        try:
            cursor = self.connection.cursor()
            
            # Get column names from first record
            columns = list(data[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)
            
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Convert data to tuples
            values = [tuple(record[col] for col in columns) for record in data]
            
            cursor.executemany(query, values)
            self.connection.commit()
            
            print(f"✅ Inserted {len(data)} records into {table_name}")
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Error inserting bulk data into {table_name}: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            if params:
                df = pd.read_sql_query(query, self.connection, params=params)
            else:
                df = pd.read_sql_query(query, self.connection)
            return df
        except sqlite3.Error as e:
            print(f"❌ Error executing query: {e}")
            return pd.DataFrame()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about table structure and row count"""
        try:
            cursor = self.connection.cursor()
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            schema = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'schema': schema,
                'row_count': row_count
            }
        except sqlite3.Error as e:
            print(f"❌ Error getting table info for {table_name}: {e}")
            return {}
    
    def clear_table(self, table_name: str) -> bool:
        """Clear all data from specified table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            self.connection.commit()
            print(f"✅ Cleared all data from {table_name}")
            return True
        except sqlite3.Error as e:
            print(f"❌ Error clearing table {table_name}: {e}")
            return False

    def drop_table(self, table_name: str) -> bool:
        """Drop specified table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()
            print(f"✅ Dropped table {table_name}")
            return True
        except sqlite3.Error as e:
            print(f"❌ Error dropping table {table_name}: {e}")
            return False
