import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from analytics.queries import PhonePeAnalytics
from utils.helpers import format_currency, format_number

# Page configuration
st.set_page_config(
    page_title="PhonePe Transaction Insights",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #5F27CD;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #5F27CD;
    }
    .insight-box {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

class PhonePeDashboard:
    def __init__(self):
        self.analytics = PhonePeAnalytics()
        
    def main(self):
        """Main dashboard function"""
        st.markdown('<h1 class="main-header">📱 PhonePe Transaction Insights Dashboard</h1>', unsafe_allow_html=True)
        
        # Add debug info in sidebar
        with st.sidebar:
            if st.checkbox("Show Debug Info"):
                self.show_debug_info()
        
        # Sidebar
        self.render_sidebar()
        
        # Main content based on selection
        page = st.session_state.get('selected_page', 'Overview')
        
        if page == 'Overview':
            self.render_overview()
        elif page == 'Transaction Analysis':
            self.render_transaction_analysis()
        elif page == 'Geographic Insights':
            self.render_geographic_insights()
        elif page == 'User Analytics':
            self.render_user_analytics()
        elif page == 'Insurance Insights':
            self.render_insurance_insights()
        elif page == 'Growth Analysis':
            self.render_growth_analysis()


    def safe_render_with_error_handling(self, render_function, page_name):
        """Safely render dashboard pages with error handling"""
        try:
            render_function()
        except Exception as e:
            st.error(f"Error loading {page_name}: {str(e)}")
            st.info("Showing simplified view...")
            
            # Fallback to basic data display
            try:
                basic_query = """
                SELECT 
                    state,
                    SUM(transaction_amount) as total_amount,
                    SUM(transaction_count) as total_count,
                    COUNT(DISTINCT transaction_type) as types
                FROM aggregated_transaction
                GROUP BY state
                ORDER BY total_amount DESC
                LIMIT 10
                """
                df = self.analytics.db_ops.execute_query(basic_query)
                
                if not df.empty:
                    st.subheader(f"📊 {page_name} - Basic View")
                    st.dataframe(df)
                    
                    fig = px.bar(df, x='state', y='total_amount', 
                            title=f"{page_name} - Top States by Transaction Amount")
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data available")
                    
            except Exception as inner_e:
                st.error(f"Critical error: {str(inner_e)}")

    def render_transaction_analysis(self):
        """Render transaction analysis dashboard - Error-free version"""
        st.markdown("## 💳 Transaction Analysis")
        
        try:
            # Get transaction type data with error checking
            type_data = self.analytics.get_transaction_type_analysis()
            
            if type_data.empty:
                st.warning("⚠️ No transaction data available. Please check database connection.")
                return
            
            # Transaction type analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Transaction Types Performance")
                try:
                    fig = px.bar(
                        type_data,
                        x='transaction_type',
                        y='total_amount',
                        title="Transaction Amount by Type",
                        color='avg_amount',
                        color_continuous_scale='plasma'
                    )
                    fig.update_xaxes(tickangle=45)
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as chart_error:
                    st.error(f"Chart error: {str(chart_error)}")
                    # Fallback to simple chart
                    fig = px.bar(type_data, x='transaction_type', y='total_amount')
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### Transaction Distribution")
                try:
                    fig = px.pie(
                        type_data,
                        values='total_amount',
                        names='transaction_type',
                        title="Transaction Amount Distribution"
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as chart_error:
                    st.error(f"Chart error: {str(chart_error)}")
            
            # Data table with safe formatting
            st.markdown("### 📋 Detailed Transaction Analysis")
            try:
                # Safe column formatting
                format_dict = {}
                if 'total_transactions' in type_data.columns:
                    format_dict['total_transactions'] = '{:,}'
                if 'total_amount' in type_data.columns:
                    format_dict['total_amount'] = '₹{:,.2f}'
                if 'avg_amount' in type_data.columns:
                    format_dict['avg_amount'] = '₹{:,.2f}'
                if 'percentage_of_total' in type_data.columns:
                    format_dict['percentage_of_total'] = '{:.2f}%'
                
                st.dataframe(type_data.style.format(format_dict), use_container_width=True)
            except Exception as table_error:
                st.warning("Showing raw data due to formatting error")
                st.dataframe(type_data, use_container_width=True)
                
        except Exception as e:
            st.error(f"❌ Error in Transaction Analysis: {str(e)}")
            st.info("💡 **Troubleshooting Steps:**")
            st.write("1. Check if database file exists")
            st.write("2. Verify data was loaded correctly")
            st.write("3. Restart the dashboard")
            
    def render_user_analytics(self):
        """Render user analytics dashboard - Error-free version"""
        st.markdown("## 👥 User Analytics")
        
        try:
            # Brand analysis with error checking
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📱 Device Brand Analysis")
                try:
                    brand_data = self.analytics.get_brand_analysis(15)
                    if not brand_data.empty:
                        fig = px.bar(
                            brand_data.head(10),  # Limit to top 10 for better display
                            x='brands',
                            y='total_users',
                            title="User Distribution by Device Brand",
                            color='total_users',
                            color_continuous_scale='viridis'
                        )
                        fig.update_xaxes(tickangle=45)
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No brand data available")
                except Exception as e:
                    st.error(f"Brand analysis error: {str(e)}")
            
            with col2:
                st.markdown("### 🎯 Market Share Overview")
                try:
                    brand_data = self.analytics.get_brand_analysis(10)
                    if not brand_data.empty:
                        fig = px.pie(
                            brand_data.head(8),
                            values='total_users',
                            names='brands',
                            title="Market Share by Brand"
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No market share data available")
                except Exception as e:
                    st.error(f"Market share error: {str(e)}")
            
            # User engagement with simplified query
            st.markdown("### 📈 User Engagement by State")
            try:
                engagement_data = self.analytics.get_user_engagement_metrics()
                if not engagement_data.empty:
                    # Simple bar chart for engagement
                    top_states = engagement_data.head(15)
                    fig = px.bar(
                        top_states,
                        x='state',
                        y='total_registered_users',
                        title="Registered Users by State",
                        color='total_registered_users',
                        color_continuous_scale='blues'
                    )
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Data table
                    st.markdown("#### 📋 User Engagement Data")
                    format_dict = {
                        'total_registered_users': '{:,}',
                        'total_app_opens': '{:,}'
                    }
                    if 'avg_opens_per_user' in engagement_data.columns:
                        format_dict['avg_opens_per_user'] = '{:.2f}'
                    
                    st.dataframe(engagement_data.style.format(format_dict), use_container_width=True)
                else:
                    st.warning("No engagement data available")
            except Exception as e:
                st.error(f"User engagement error: {str(e)}")
                
        except Exception as e:
            st.error(f"❌ Error in User Analytics: {str(e)}")
    
    def show_debug_info(self):
        """Show debug information for troubleshooting"""
        with st.expander("🔧 Debug Information", expanded=False):
            try:
                # Check database connection
                overview = self.analytics.get_transaction_overview()
                st.success("✅ Database connection working")
                st.write("Database stats:", overview)
                
                # Check data availability
                tables = ['aggregated_transaction', 'aggregated_user', 'aggregated_insurance']
                for table in tables:
                    try:
                        query = f"SELECT COUNT(*) as count FROM {table}"
                        result = self.analytics.db_ops.execute_query(query)
                        count = result.iloc[0]['count'] if not result.empty else 0
                        st.write(f"📊 {table}: {count:,} records")
                    except Exception as e:
                        st.error(f"❌ {table}: {str(e)}")
                        
            except Exception as e:
                st.error(f"❌ Debug info error: {str(e)}")
                
    def render_growth_analysis(self):
        """Render growth analysis dashboard - Error-free version"""
        st.markdown("## 📈 Growth Analysis")
        
        try:
            growth_data = self.analytics.get_growth_analysis()
            
            if growth_data.empty:
                st.warning("⚠️ No growth data available")
                return
            
            # Show yearly totals first (always works)
            st.markdown("### 📊 Yearly Performance Overview")
            col1, col2 = st.columns(2)
            
            with col1:
                try:
                    fig = px.bar(
                        growth_data,
                        x='year',
                        y='total_amount',
                        title="Total Transaction Amount by Year",
                        color='total_amount',
                        color_continuous_scale='blues'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Amount chart error: {str(e)}")
            
            with col2:
                try:
                    fig = px.bar(
                        growth_data,
                        x='year',
                        y='total_transactions',
                        title="Total Transaction Count by Year",
                        color='total_transactions',
                        color_continuous_scale='greens'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Count chart error: {str(e)}")
            
            # Growth percentage analysis (only if data exists)
            st.markdown("### 📈 Growth Trends")
            
            # Filter data with growth percentages
            growth_with_data = growth_data.dropna(subset=['amount_growth_percent', 'transaction_growth_percent'])
            
            if not growth_with_data.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    try:
                        fig = px.line(
                            growth_with_data,
                            x='year',
                            y='amount_growth_percent',
                            title="Year-over-Year Amount Growth (%)",
                            markers=True
                        )
                        fig.update_yaxis(title="Growth Percentage (%)")
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.info("Growth trend visualization unavailable")
                
                with col2:
                    try:
                        fig = px.line(
                            growth_with_data,
                            x='year',
                            y='transaction_growth_percent',
                            title="Year-over-Year Transaction Growth (%)",
                            markers=True,
                            line_shape='spline'
                        )
                        fig.update_yaxis(title="Growth Percentage (%)")
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.info("Transaction growth visualization unavailable")
            else:
                st.info("💡 Growth percentages need at least 2 years of data to calculate")
            
            # Data table
            st.markdown("### 📋 Detailed Growth Metrics")
            try:
                format_dict = {
                    'total_transactions': '{:,}',
                    'total_amount': '₹{:,.2f}',
                    'active_states': '{:,}'
                }
                # Add growth percentage formatting if columns exist
                if 'amount_growth_percent' in growth_data.columns:
                    format_dict['amount_growth_percent'] = '{:.2f}%'
                if 'transaction_growth_percent' in growth_data.columns:
                    format_dict['transaction_growth_percent'] = '{:.2f}%'
                
                st.dataframe(growth_data.style.format(format_dict), use_container_width=True)
            except Exception as e:
                st.warning("Showing raw growth data")
                st.dataframe(growth_data, use_container_width=True)
                
        except Exception as e:
            st.error(f"❌ Error in Growth Analysis: {str(e)}")
            st.info("💡 This usually means insufficient data for growth calculations")
            
    def render_sidebar(self):
        """Render sidebar navigation"""
        st.sidebar.markdown("## 🧭 Navigation")
        
        pages = [
            'Overview',
            'Transaction Analysis', 
            'Geographic Insights',
            'User Analytics',
            'Insurance Insights',
            'Growth Analysis'
        ]
        
        selected_page = st.sidebar.selectbox(
            "Select Dashboard Page",
            pages,
            key='selected_page'
        )
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("## 📊 Quick Stats")
        
        # Quick stats
        overview_data = self.analytics.get_transaction_overview()
        if overview_data:
            st.sidebar.metric(
                "Total Transactions", 
                format_number(overview_data.get('total_transaction_count', 0))
            )
            st.sidebar.metric(
                "Total Amount", 
                format_currency(overview_data.get('total_transaction_amount', 0))
            )
            st.sidebar.metric(
                "Active States", 
                overview_data.get('unique_states', 0)
            )
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Developed by:** Data Analytics Team")
        st.sidebar.markdown("**Version:** 1.0.0")
    
    def render_overview(self):
        """Render overview dashboard"""
        st.markdown("## 📈 Business Overview")
        
        # Get overview data
        overview_data = self.analytics.get_transaction_overview()
        
        if overview_data:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Transactions",
                    format_number(overview_data.get('total_transaction_count', 0)),
                    delta=None
                )
            
            with col2:
                st.metric(
                    "Total Amount",
                    format_currency(overview_data.get('total_transaction_amount', 0)),
                    delta=None
                )
            
            with col3:
                st.metric(
                    "Average Transaction",
                    format_currency(overview_data.get('avg_transaction_amount', 0)),
                    delta=None
                )
            
            with col4:
                st.metric(
                    "Active States",
                    overview_data.get('unique_states', 0),
                    delta=None
                )
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🏆 Top States by Transaction Volume")
            state_data = self.analytics.get_state_wise_analysis(10)
            if not state_data.empty:
                fig = px.bar(
                    state_data.head(10),
                    x='total_amount',
                    y='state',
                    orientation='h',
                    title="Top 10 States by Transaction Amount",
                    color='total_amount',
                    color_continuous_scale='viridis'
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Transaction Types Distribution")
            type_data = self.analytics.get_transaction_type_analysis()
            if not type_data.empty:
                fig = px.pie(
                    type_data,
                    values='total_amount',
                    names='transaction_type',
                    title="Transaction Distribution by Type"
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
        
        # Quarterly trends
        st.markdown("### 📅 Quarterly Performance Trends")
        quarterly_data = self.analytics.get_quarterly_trends()
        if not quarterly_data.empty:
            fig = px.line(
                quarterly_data,
                x='quarter',
                y='total_amount',
                color='year',
                title="Quarterly Transaction Amount Trends",
                markers=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    
    def render_geographic_insights(self):
        """Render geographic insights dashboard"""
        st.markdown("## 🗺️ Geographic Insights")
        
        # State selection
        state_data = self.analytics.get_state_wise_analysis(50)
        states = state_data['state'].tolist() if not state_data.empty else []
        
        selected_state = st.selectbox("Select State for Detailed Analysis", ['All States'] + states)
        
        if selected_state == 'All States':
            # All states analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🏆 Top Performing States")
                if not state_data.empty:
                    fig = px.choropleth(
                        state_data.head(20),
                        locations='state',
                        color='total_amount',
                        hover_name='state',
                        hover_data=['total_transactions', 'avg_amount'],
                        title="State-wise Transaction Heatmap",
                        color_continuous_scale='viridis'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📊 State Performance Metrics")
                if not state_data.empty:
                    fig = px.scatter(
                        state_data.head(20),
                        x='total_transactions',
                        y='total_amount',
                        size='avg_amount',
                        color='years_active',
                        hover_name='state',
                        title="Transaction Volume vs Amount by State"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        else:
            # Individual state analysis
            st.markdown(f"### 📍 Detailed Analysis: {selected_state}")
            
            # Get comprehensive state report
            state_report = self.analytics.get_comprehensive_state_report(selected_state)
            
            if state_report:
                # District performance
                col1, col2 = st.columns(2)
                
                with col1:
                    if not state_report['districts'].empty:
                        st.markdown("#### Top Districts")
                        fig = px.bar(
                            state_report['districts'].head(10),
                            x='total_amount',
                            y='district',
                            orientation='h',
                            title=f"Top Districts in {selected_state}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    if not state_report['users'].empty:
                        st.markdown("#### Device Brand Preferences")
                        fig = px.pie(
                            state_report['users'],
                            values='total_users',
                            names='brands',
                            title=f"Brand Distribution in {selected_state}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
    
    
    def render_insurance_insights(self):
        """Render insurance insights dashboard"""
        st.markdown("## 🛡️ Insurance Insights")
        
        insurance_data = self.analytics.get_insurance_insights()
        
        if not insurance_data.empty:
            # Key metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_policies = insurance_data['total_policies'].sum()
                st.metric("Total Policies", format_number(total_policies))
            
            with col2:
                total_premium = insurance_data['total_premium'].sum()
                st.metric("Total Premium", format_currency(total_premium))
            
            with col3:
                avg_premium = insurance_data['avg_premium'].mean()
                st.metric("Average Premium", format_currency(avg_premium))
            
            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Insurance Type Distribution")
                fig = px.pie(
                    insurance_data,
                    values='total_premium',
                    names='insurance_type',
                    title="Premium Distribution by Insurance Type"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### Policy Count vs Premium")
                fig = px.scatter(
                    insurance_data,
                    x='total_policies',
                    y='avg_premium',
                    size='total_premium',
                    color='insurance_type',
                    title="Policy Volume vs Average Premium",
                    hover_data=['states_covered']
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Market analysis
            st.markdown("### 📊 Insurance Market Analysis")
            fig = px.bar(
                insurance_data,
                x='insurance_type',
                y='total_premium',
                title="Total Premium by Insurance Type",
                color='avg_premium',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    
# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = PhonePeDashboard()
    dashboard.main()
