import pandas as pd
import numpy as np
from typing import Union, List, Dict, Any

def format_currency(amount: Union[int, float]) -> str:
    """Format number as Indian currency"""
    if pd.isna(amount):
        return "₹0.00"
    
    if amount >= 10000000:  # 1 crore
        return f"₹{amount/10000000:.2f} Cr"
    elif amount >= 100000:  # 1 lakh
        return f"₹{amount/100000:.2f} L"
    elif amount >= 1000:  # 1 thousand
        return f"₹{amount/1000:.2f} K"
    else:
        return f"₹{amount:.2f}"

def format_number(number: Union[int, float]) -> str:
    """Format large numbers with Indian numbering system"""
    if pd.isna(number):
        return "0"
    
    if number >= 10000000:  # 1 crore
        return f"{number/10000000:.2f} Cr"
    elif number >= 100000:  # 1 lakh
        return f"{number/100000:.2f} L"
    elif number >= 1000:  # 1 thousand
        return f"{number/1000:.2f} K"
    else:
        return f"{int(number):,}"

def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100

def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate data quality and return report"""
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': df.dtypes.to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum(),
        'quality_score': 0.0
    }
    
    # Calculate quality score
    missing_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    duplicate_percentage = (report['duplicate_rows'] / len(df)) * 100
    
    quality_score = max(0, 100 - missing_percentage - duplicate_percentage)
    report['quality_score'] = round(quality_score, 2)
    
    return report

def get_color_palette(n_colors: int) -> List[str]:
    """Get a color palette with specified number of colors"""
    colors = [
        '#5F27CD', '#00d2d3', '#ff9ff3', '#54a0ff', '#5f27cd',
        '#00d2d3', '#ff9ff3', '#54a0ff', '#48dbfb', '#0abde3',
        '#006ba6', '#0496c7', '#fcbf49', '#f77f00', '#d62828'
    ]
    
    if n_colors <= len(colors):
        return colors[:n_colors]
    else:
        # Repeat colors if more needed
        return (colors * ((n_colors // len(colors)) + 1))[:n_colors]

def export_to_excel(data_dict: Dict[str, pd.DataFrame], filename: str) -> bool:
    """Export multiple DataFrames to Excel with different sheets"""
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return True
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        return False

def create_summary_stats(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    """Create summary statistics for numeric columns"""
    summary = df[numeric_columns].describe()
    
    # Add additional statistics
    summary.loc['missing'] = df[numeric_columns].isnull().sum()
    summary.loc['missing_pct'] = (df[numeric_columns].isnull().sum() / len(df)) * 100
    summary.loc['unique'] = df[numeric_columns].nunique()
    
    return summary

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize column names"""
    df_clean = df.copy()
    
    # Convert to lowercase and replace spaces with underscores
    df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_')
    
    # Remove special characters
    df_clean.columns = df_clean.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
    
    return df_clean

def get_top_insights(df: pd.DataFrame, metric_column: str, group_column: str, top_n: int = 5) -> List[str]:
    """Generate top insights from data"""
    insights = []
    
    try:
        # Top performers
        top_data = df.groupby(group_column)[metric_column].sum().sort_values(ascending=False).head(top_n)
        
        insights.append(f"Top {group_column} by {metric_column}:")
        for idx, (name, value) in enumerate(top_data.items(), 1):
            insights.append(f"{idx}. {name}: {format_currency(value) if 'amount' in metric_column.lower() else format_number(value)}")
        
        # Overall statistics
        total = df[metric_column].sum()
        average = df[metric_column].mean()
        
        insights.append(f"\nOverall Statistics:")
        insights.append(f"Total {metric_column}: {format_currency(total) if 'amount' in metric_column.lower() else format_number(total)}")
        insights.append(f"Average {metric_column}: {format_currency(average) if 'amount' in metric_column.lower() else format_number(average)}")
        
    except Exception as e:
        insights.append(f"Error generating insights: {e}")
    
    return insights
