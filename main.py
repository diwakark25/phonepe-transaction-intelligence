import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database.schema import DatabaseManager
from database.operations import DatabaseOperations
from data.data_generator import PhonePeDataGenerator
from analytics.queries import PhonePeAnalytics

def setup_database():
    """Setup database with tables and sample data"""
    print("🚀 Setting up PhonePe Transaction Insights Database...")
    
    # Initialize database
    db_manager = DatabaseManager()
    db_ops = DatabaseOperations()
    
    try:
        # Connect to database
        db_manager.connect()
        db_ops.connect()
        
        print("📊 Creating database tables...")
        db_manager.create_tables()
        
        # Generate and insert sample data
        print("🔄 Generating sample data...")
        data_generator = PhonePeDataGenerator()
        
        # Generate data for all tables
        tables_data = {
            'aggregated_transaction': data_generator.generate_aggregated_transaction_data(1500),
            'aggregated_user': data_generator.generate_aggregated_user_data(1200),
            'aggregated_insurance': data_generator.generate_aggregated_insurance_data(800),
            'map_transaction': data_generator.generate_map_transaction_data(1500),
            'map_user': data_generator.generate_map_user_data(1200),
            'map_insurance': data_generator.generate_map_insurance_data(1000),
            'top_transaction': data_generator.generate_top_transaction_data(1200),
            'top_user': data_generator.generate_top_user_data(1000),
            'top_insurance': data_generator.generate_top_insurance_data(800)
        }
        
        # Insert data into tables
        for table_name, data in tables_data.items():
            print(f"📥 Inserting data into {table_name}...")
            db_ops.insert_bulk_data(table_name, data)
        
        print("✅ Database setup completed successfully!")
        
        # Display summary
        print("\n📈 Database Summary:")
        for table_name in tables_data.keys():
            info = db_ops.get_table_info(table_name)
            print(f"  {table_name}: {info.get('row_count', 0)} records")
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False
    
    finally:
        db_manager.close()
        db_ops.close()
    
    return True

def run_sample_analytics():
    """Run sample analytics queries"""
    print("\n🔍 Running Sample Analytics...")
    
    analytics = PhonePeAnalytics()
    
    try:
        # Transaction overview
        print("\n📊 Transaction Overview:")
        overview = analytics.get_transaction_overview()
        for key, value in overview.items():
            print(f"  {key}: {value}")
        
        # Top states
        print("\n🏆 Top 5 States by Transaction Amount:")
        top_states = analytics.get_state_wise_analysis(5)
        for _, row in top_states.iterrows():
            print(f"  {row['state']}: ₹{row['total_amount']:,.2f}")
        
        # Transaction types
        print("\n💳 Transaction Type Analysis:")
        tx_types = analytics.get_transaction_type_analysis()
        for _, row in tx_types.iterrows():
            print(f"  {row['transaction_type']}: {row['percentage_of_total']:.2f}%")
        
        print("\n✅ Sample analytics completed!")
        
    except Exception as e:
        print(f"❌ Error running analytics: {e}")
    
    finally:
        analytics.close_connection()

def main():
    """Main application entry point"""
    print("=" * 60)
    print("📱 PhonePe Transaction Insights - Professional Implementation")
    print("=" * 60)
    
    # Check if database exists
    db_path = "phonepe_insights.db"
    if not os.path.exists(db_path):
        setup_database()
    else:
        print("📊 Database already exists. Skipping setup.")
        print("🗑️  Delete 'phonepe_insights.db' to regenerate sample data.")
    
    # Run sample analytics
    run_sample_analytics()
    
    print("\n🚀 To launch the Streamlit dashboard, run:")
    print("   streamlit run dashboard/streamlit_app.py")
    print("\n📝 For more detailed analysis, explore the analytics module.")
    print("=" * 60)

if __name__ == "__main__":
    main()
