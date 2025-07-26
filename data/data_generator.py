import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

class PhonePeDataGenerator:
    def __init__(self):
        self.states = [
            'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh',
            'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka',
            'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
            'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu',
            'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal',
            'Delhi', 'Jammu and Kashmir', 'Ladakh', 'Chandigarh', 'Dadra and Nagar Haveli',
            'Daman and Diu', 'Lakshadweep', 'Puducherry'
        ]
        
        self.transaction_types = [
            'Peer-to-peer payments', 'Merchant payments', 'Financial Services',
            'Recharge & bill payments', 'Others'
        ]
        
        self.insurance_types = [
            'Life Insurance', 'Health Insurance', 'Vehicle Insurance',
            'Travel Insurance', 'Property Insurance'
        ]
        
        self.brands = [
            'Samsung', 'Xiaomi', 'Vivo', 'Oppo', 'OnePlus', 'Realme',
            'Apple', 'Huawei', 'Motorola', 'Nokia', 'Others'
        ]
        
        self.years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
        self.quarters = [1, 2, 3, 4]
    
    def generate_aggregated_transaction_data(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate sample aggregated transaction data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'transaction_type': random.choice(self.transaction_types),
                'transaction_count': random.randint(1000, 100000),
                'transaction_amount': round(random.uniform(10000, 10000000), 2)
            }
            data.append(record)
        return data
    
    def generate_aggregated_user_data(self, num_records: int = 800) -> List[Dict[str, Any]]:
        """Generate sample aggregated user data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'brands': random.choice(self.brands),
                'count': random.randint(100, 50000),
                'percentage': round(random.uniform(1.0, 25.0), 2)
            }
            data.append(record)
        return data
    
    def generate_aggregated_insurance_data(self, num_records: int = 600) -> List[Dict[str, Any]]:
        """Generate sample aggregated insurance data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'insurance_type': random.choice(self.insurance_types),
                'insurance_count': random.randint(50, 10000),
                'insurance_amount': round(random.uniform(5000, 5000000), 2)
            }
            data.append(record)
        return data
    
    def generate_map_transaction_data(self, num_records: int = 1200) -> List[Dict[str, Any]]:
        """Generate sample map transaction data"""
        districts = {
            'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad'],
            'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore', 'Belgaum'],
            'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem', 'Tiruchirappalli'],
            'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Bhavnagar'],
            'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota', 'Bikaner']
        }
        
        data = []
        for _ in range(num_records):
            state = random.choice(list(districts.keys()))
            record = {
                'state': state,
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'district': random.choice(districts[state]),
                'count': random.randint(500, 50000),
                'amount': round(random.uniform(25000, 2500000), 2)
            }
            data.append(record)
        return data
    
    def generate_map_user_data(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate sample map user data"""
        districts = {
            'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad'],
            'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore', 'Belgaum'],
            'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem', 'Tiruchirappalli'],
            'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Bhavnagar'],
            'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota', 'Bikaner']
        }
        
        data = []
        for _ in range(num_records):
            state = random.choice(list(districts.keys()))
            record = {
                'state': state,
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'district': random.choice(districts[state]),
                'registered_users': random.randint(1000, 100000),
                'app_opens': random.randint(5000, 500000)
            }
            data.append(record)
        return data
    
    def generate_map_insurance_data(self, num_records: int = 800) -> List[Dict[str, Any]]:
        """Generate sample map insurance data"""
        districts = {
            'Maharashtra': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad'],
            'Karnataka': ['Bangalore', 'Mysore', 'Hubli', 'Mangalore', 'Belgaum'],
            'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem', 'Tiruchirappalli'],
            'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Bhavnagar'],
            'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota', 'Bikaner']
        }
        
        data = []
        for _ in range(num_records):
            state = random.choice(list(districts.keys()))
            record = {
                'state': state,
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'district': random.choice(districts[state]),
                'insurance_count': random.randint(100, 5000),
                'insurance_amount': round(random.uniform(10000, 1000000), 2)
            }
            data.append(record)
        return data
    
    def generate_top_transaction_data(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate sample top transaction data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'pincode': str(random.randint(100000, 999999)),
                'transaction_count': random.randint(100, 10000),
                'transaction_amount': round(random.uniform(5000, 500000), 2)
            }
            data.append(record)
        return data
    
    def generate_top_user_data(self, num_records: int = 800) -> List[Dict[str, Any]]:
        """Generate sample top user data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'pincode': str(random.randint(100000, 999999)),
                'registered_users': random.randint(500, 25000)
            }
            data.append(record)
        return data
    
    def generate_top_insurance_data(self, num_records: int = 600) -> List[Dict[str, Any]]:
        """Generate sample top insurance data"""
        data = []
        for _ in range(num_records):
            record = {
                'state': random.choice(self.states),
                'year': random.choice(self.years),
                'quarter': random.choice(self.quarters),
                'pincode': str(random.randint(100000, 999999)),
                'insurance_count': random.randint(50, 2000),
                'insurance_amount': round(random.uniform(2500, 250000), 2)
            }
            data.append(record)
        return data
