from database.operations import DatabaseOperations
import pandas as pd
from typing import Dict, Any, Optional, List
import sqlite3

class PhonePeAnalytics:
    def __init__(self, db_path: str = "phonepe_insights.db"):
        self.db_ops = DatabaseOperations(db_path)
        self.db_ops.connect()
    
    def get_transaction_overview(self) -> Dict[str, Any]:
        """Get overall transaction statistics"""
        query = """
        SELECT 
            COUNT(*) as total_transactions,
            SUM(transaction_count) as total_transaction_count,
            SUM(transaction_amount) as total_transaction_amount,
            AVG(transaction_amount) as avg_transaction_amount,
            COUNT(DISTINCT state) as unique_states,
            COUNT(DISTINCT transaction_type) as unique_transaction_types,
            MIN(year) as earliest_year,
            MAX(year) as latest_year
        FROM aggregated_transaction
        """
        result = self.db_ops.execute_query(query)
        return result.to_dict('records')[0] if not result.empty else {}
    
    def get_state_wise_analysis(self, limit: int = 10) -> pd.DataFrame:
        """Get top states by transaction volume and amount"""
        query = """
        SELECT 
            state,
            SUM(transaction_count) as total_transactions,
            SUM(transaction_amount) as total_amount,
            AVG(transaction_amount) as avg_amount,
            COUNT(DISTINCT year) as years_active,
            COUNT(DISTINCT transaction_type) as transaction_types,
            ROUND(
                (SUM(transaction_amount) * 100.0 / 
                (SELECT SUM(transaction_amount) FROM aggregated_transaction)), 2
            ) as percentage_of_total
        FROM aggregated_transaction
        GROUP BY state
        ORDER BY total_amount DESC
        LIMIT ?
        """
        return self.db_ops.execute_query(query, (limit,))
    
    def get_quarterly_trends(self, year: Optional[int] = None) -> pd.DataFrame:
        """Get quarterly transaction trends"""
        if year:
            query = """
            SELECT 
                quarter,
                SUM(transaction_count) as total_transactions,
                SUM(transaction_amount) as total_amount,
                AVG(transaction_amount) as avg_amount,
                COUNT(DISTINCT state) as states_active,
                COUNT(DISTINCT transaction_type) as types_active
            FROM aggregated_transaction
            WHERE year = ?
            GROUP BY quarter
            ORDER BY quarter
            """
            return self.db_ops.execute_query(query, (year,))
        else:
            query = """
            SELECT 
                year,
                quarter,
                SUM(transaction_count) as total_transactions,
                SUM(transaction_amount) as total_amount,
                AVG(transaction_amount) as avg_amount,
                COUNT(DISTINCT state) as states_active
            FROM aggregated_transaction
            GROUP BY year, quarter
            ORDER BY year, quarter
            """
            return self.db_ops.execute_query(query)
    
    def get_transaction_type_analysis(self) -> pd.DataFrame:
        """Analyze transaction types comprehensively"""
        query = """
        SELECT 
            transaction_type,
            SUM(transaction_count) as total_transactions,
            SUM(transaction_amount) as total_amount,
            AVG(transaction_amount) as avg_amount,
            MIN(transaction_amount) as min_amount,
            MAX(transaction_amount) as max_amount,
            COUNT(DISTINCT state) as states_covered,
            COUNT(DISTINCT year) as years_active,
            ROUND(
                (SUM(transaction_amount) * 100.0 / 
                (SELECT SUM(transaction_amount) FROM aggregated_transaction)), 2
            ) as percentage_of_total,
            ROUND(
                (SUM(transaction_count) * 100.0 / 
                (SELECT SUM(transaction_count) FROM aggregated_transaction)), 2
            ) as volume_percentage
        FROM aggregated_transaction
        GROUP BY transaction_type
        ORDER BY total_amount DESC
        """
        return self.db_ops.execute_query(query)
    
    def get_brand_analysis(self, limit: int = 15) -> pd.DataFrame:
        """Analyze user preferences by device brands"""
        query = """
        SELECT 
            brands,
            SUM(count) as total_users,
            AVG(percentage) as avg_market_share,
            MIN(percentage) as min_market_share,
            MAX(percentage) as max_market_share,
            COUNT(DISTINCT state) as states_present,
            COUNT(DISTINCT year) as years_active,
            ROUND(
                (SUM(count) * 100.0 / 
                (SELECT SUM(count) FROM aggregated_user)), 2
            ) as overall_market_share
        FROM aggregated_user
        GROUP BY brands
        ORDER BY total_users DESC
        LIMIT ?
        """
        return self.db_ops.execute_query(query, (limit,))
    
    def get_insurance_insights(self) -> pd.DataFrame:
        """Get comprehensive insurance adoption insights"""
        query = """
        SELECT 
            insurance_type,
            SUM(insurance_count) as total_policies,
            SUM(insurance_amount) as total_premium,
            AVG(insurance_amount) as avg_premium,
            MIN(insurance_amount) as min_premium,
            MAX(insurance_amount) as max_premium,
            COUNT(DISTINCT state) as states_covered,
            COUNT(DISTINCT year) as years_active,
            ROUND(
                (SUM(insurance_amount) * 100.0 / 
                (SELECT SUM(insurance_amount) FROM aggregated_insurance)), 2
            ) as premium_percentage,
            ROUND(
                (SUM(insurance_count) * 100.0 / 
                (SELECT SUM(insurance_count) FROM aggregated_insurance)), 2
            ) as policy_percentage
        FROM aggregated_insurance
        GROUP BY insurance_type
        ORDER BY total_premium DESC
        """
        return self.db_ops.execute_query(query)
    
    def get_district_performance(self, state: str, limit: int = 10) -> pd.DataFrame:
        """Get top performing districts in a state"""
        query = """
        SELECT 
            district,
            SUM(count) as total_transactions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            COUNT(DISTINCT year) as years_active,
            COUNT(DISTINCT quarter) as quarters_active,
            ROUND(
                (SUM(amount) * 100.0 / 
                (SELECT SUM(amount) FROM map_transaction WHERE state = ?)), 2
            ) as state_percentage
        FROM map_transaction
        WHERE state = ?
        GROUP BY district
        ORDER BY total_amount DESC
        LIMIT ?
        """
        return self.db_ops.execute_query(query, (state, state, limit))
    
    def get_user_engagement_metrics(self, state: Optional[str] = None) -> pd.DataFrame:
        """Get comprehensive user engagement metrics"""
        if state:
            query = """
            SELECT 
                district,
                SUM(registered_users) as total_registered_users,
                SUM(app_opens) as total_app_opens,
                ROUND(AVG(CAST(app_opens AS FLOAT) / NULLIF(registered_users, 0)), 2) as avg_opens_per_user,
                MIN(registered_users) as min_users,
                MAX(registered_users) as max_users,
                COUNT(DISTINCT year) as years_active,
                ROUND(
                    (SUM(registered_users) * 100.0 / 
                    (SELECT SUM(registered_users) FROM map_user WHERE state = ?)), 2
                ) as district_user_percentage
            FROM map_user
            WHERE state = ?
            GROUP BY district
            ORDER BY total_registered_users DESC
            """
            return self.db_ops.execute_query(query, (state, state))
        else:
            query = """
            SELECT 
                state,
                SUM(registered_users) as total_registered_users,
                SUM(app_opens) as total_app_opens,
                ROUND(AVG(CAST(app_opens AS FLOAT) / NULLIF(registered_users, 0)), 2) as avg_opens_per_user,
                COUNT(DISTINCT district) as districts_covered,
                COUNT(DISTINCT year) as years_active,
                ROUND(
                    (SUM(registered_users) * 100.0 / 
                    (SELECT SUM(registered_users) FROM map_user)), 2
                ) as national_user_percentage
            FROM map_user
            GROUP BY state
            ORDER BY total_registered_users DESC
            """
            return self.db_ops.execute_query(query)
    
    def get_top_pincodes(self, metric: str = 'transaction_amount', limit: int = 20) -> pd.DataFrame:
        """Get top performing pincodes by specified metric"""
        if metric == 'transaction_amount':
            query = """
            SELECT 
                state,
                pincode,
                SUM(transaction_count) as total_transactions,
                SUM(transaction_amount) as total_amount,
                AVG(transaction_amount) as avg_amount,
                COUNT(DISTINCT year) as years_active,
                COUNT(DISTINCT quarter) as quarters_active
            FROM top_transaction
            GROUP BY state, pincode
            ORDER BY total_amount DESC
            LIMIT ?
            """
        else:  # registered_users
            query = """
            SELECT 
                state,
                pincode,
                SUM(registered_users) as total_users,
                COUNT(DISTINCT year) as years_active,
                COUNT(DISTINCT quarter) as quarters_active
            FROM top_user
            GROUP BY state, pincode
            ORDER BY total_users DESC
            LIMIT ?
            """
        return self.db_ops.execute_query(query, (limit,))
    
    def get_growth_analysis(self) -> pd.DataFrame:
        """Comprehensive year-over-year growth analysis"""
        query = """
        WITH yearly_data AS (
            SELECT 
                year,
                SUM(transaction_count) as total_transactions,
                SUM(transaction_amount) as total_amount,
                COUNT(DISTINCT state) as active_states,
                COUNT(DISTINCT transaction_type) as active_types
            FROM aggregated_transaction
            GROUP BY year
        ),
        growth_calc AS (
            SELECT 
                year,
                total_transactions,
                total_amount,
                active_states,
                active_types,
                LAG(total_amount) OVER (ORDER BY year) as prev_year_amount,
                LAG(total_transactions) OVER (ORDER BY year) as prev_year_transactions,
                LAG(active_states) OVER (ORDER BY year) as prev_year_states
            FROM yearly_data
        )
        SELECT 
            year,
            total_transactions,
            total_amount,
            active_states,
            active_types,
            CASE 
                WHEN prev_year_amount IS NOT NULL AND prev_year_amount > 0 THEN
                    ROUND(((total_amount - prev_year_amount) * 100.0 / prev_year_amount), 2)
                ELSE NULL
            END as amount_growth_percent,
            CASE 
                WHEN prev_year_transactions IS NOT NULL AND prev_year_transactions > 0 THEN
                    ROUND(((total_transactions - prev_year_transactions) * 100.0 / prev_year_transactions), 2)
                ELSE NULL
            END as transaction_growth_percent,
            CASE 
                WHEN prev_year_states IS NOT NULL AND prev_year_states > 0 THEN
                    ROUND(((active_states - prev_year_states) * 100.0 / prev_year_states), 2)
                ELSE NULL
            END as state_expansion_percent
        FROM growth_calc
        ORDER BY year
        """
        return self.db_ops.execute_query(query)
    
    def get_seasonal_analysis(self) -> pd.DataFrame:
        """Analyze seasonal patterns in transactions (SQLite compatible)"""
        query = """
        SELECT 
            quarter,
            AVG(transaction_count) as avg_transactions,
            AVG(transaction_amount) as avg_amount,
            MIN(transaction_amount) as min_amount,
            MAX(transaction_amount) as max_amount,
            COUNT(DISTINCT state) as states_active,
            COUNT(DISTINCT year) as years_analyzed
        FROM aggregated_transaction
        GROUP BY quarter
        ORDER BY quarter
        """
        return self.db_ops.execute_query(query)
    
    def get_market_concentration_analysis(self) -> pd.DataFrame:
        """Analyze market concentration using Pareto principle"""
        query = """
        WITH state_totals AS (
            SELECT 
                state,
                SUM(transaction_amount) as total_amount
            FROM aggregated_transaction
            GROUP BY state
        ),
        ranked_states AS (
            SELECT 
                state,
                total_amount,
                ROW_NUMBER() OVER (ORDER BY total_amount DESC) as rank,
                SUM(total_amount) OVER () as grand_total
            FROM state_totals
        ),
        cumulative_analysis AS (
            SELECT 
                rank,
                state,
                total_amount,
                ROUND((total_amount * 100.0 / grand_total), 2) as individual_percentage,
                ROUND((SUM(total_amount) OVER (ORDER BY rank) * 100.0 / grand_total), 2) as cumulative_percentage
            FROM ranked_states
        )
        SELECT 
            rank,
            state,
            total_amount,
            individual_percentage,
            cumulative_percentage,
            CASE 
                WHEN cumulative_percentage <= 80 THEN 'Top 80%'
                WHEN cumulative_percentage <= 95 THEN 'Next 15%'
                ELSE 'Bottom 5%'
            END as market_segment
        FROM cumulative_analysis
        ORDER BY rank
        """
        return self.db_ops.execute_query(query)
    
    def get_comprehensive_state_report(self, state: str) -> Dict[str, pd.DataFrame]:
        """Get comprehensive report for a specific state"""
        report = {}
        
        # Transaction analysis by type and time
        query1 = """
        SELECT 
            year,
            quarter,
            transaction_type,
            SUM(transaction_count) as transactions,
            SUM(transaction_amount) as amount,
            AVG(transaction_amount) as avg_amount
        FROM aggregated_transaction
        WHERE state = ?
        GROUP BY year, quarter, transaction_type
        ORDER BY year, quarter, amount DESC
        """
        report['transactions'] = self.db_ops.execute_query(query1, (state,))
        
        # User brand preferences over time
        query2 = """
        SELECT 
            year,
            brands,
            SUM(count) as total_users,
            AVG(percentage) as avg_percentage,
            MIN(percentage) as min_percentage,
            MAX(percentage) as max_percentage
        FROM aggregated_user
        WHERE state = ?
        GROUP BY year, brands
        ORDER BY year, total_users DESC
        """
        report['users'] = self.db_ops.execute_query(query2, (state,))
        
        # District-wise performance analysis
        query3 = """
        SELECT 
            district,
            SUM(mt.count) as total_transactions,
            SUM(mt.amount) as total_amount,
            AVG(mt.amount) as avg_amount,
            SUM(mu.registered_users) as total_users,
            SUM(mu.app_opens) as total_app_opens,
            ROUND(AVG(CAST(mu.app_opens AS FLOAT) / NULLIF(mu.registered_users, 0)), 2) as avg_opens_per_user
        FROM map_transaction mt
        LEFT JOIN map_user mu ON mt.state = mu.state AND mt.district = mu.district 
            AND mt.year = mu.year AND mt.quarter = mu.quarter
        WHERE mt.state = ?
        GROUP BY district
        ORDER BY total_amount DESC
        """
        report['districts'] = self.db_ops.execute_query(query3, (state,))
        
        # Insurance adoption analysis
        query4 = """
        SELECT 
            year,
            insurance_type,
            SUM(insurance_count) as policies,
            SUM(insurance_amount) as premium,
            AVG(insurance_amount) as avg_premium
        FROM aggregated_insurance
        WHERE state = ?
        GROUP BY year, insurance_type
        ORDER BY year, premium DESC
        """
        report['insurance'] = self.db_ops.execute_query(query4, (state,))
        
        # Top performing pincodes
        query5 = """
        SELECT 
            pincode,
            SUM(tt.transaction_count) as total_transactions,
            SUM(tt.transaction_amount) as total_amount,
            SUM(tu.registered_users) as total_users
        FROM top_transaction tt
        LEFT JOIN top_user tu ON tt.state = tu.state AND tt.pincode = tu.pincode 
            AND tt.year = tu.year AND tt.quarter = tu.quarter
        WHERE tt.state = ?
        GROUP BY pincode
        ORDER BY total_amount DESC
        LIMIT 10
        """
        report['top_pincodes'] = self.db_ops.execute_query(query5, (state,))
        
        return report
    
    def get_correlation_analysis(self) -> pd.DataFrame:
        """Analyze correlations between different metrics"""
        query = """
        SELECT 
            at.state,
            at.year,
            at.quarter,
            SUM(at.transaction_amount) as transaction_amount,
            SUM(at.transaction_count) as transaction_count,
            AVG(au.count) as avg_user_count,
            SUM(ai.insurance_amount) as insurance_amount,
            SUM(mu.registered_users) as registered_users,
            SUM(mu.app_opens) as app_opens
        FROM aggregated_transaction at
        LEFT JOIN aggregated_user au ON at.state = au.state AND at.year = au.year AND at.quarter = au.quarter
        LEFT JOIN aggregated_insurance ai ON at.state = ai.state AND at.year = ai.year AND at.quarter = ai.quarter
        LEFT JOIN (
            SELECT state, year, quarter, SUM(registered_users) as registered_users, SUM(app_opens) as app_opens
            FROM map_user
            GROUP BY state, year, quarter
        ) mu ON at.state = mu.state AND at.year = mu.year AND at.quarter = mu.quarter
        GROUP BY at.state, at.year, at.quarter
        ORDER BY at.state, at.year, at.quarter
        """
        return self.db_ops.execute_query(query)
    
    def get_advanced_kpis(self) -> Dict[str, Any]:
        """Calculate advanced KPIs and business metrics"""
        kpis = {}
        
        # Transaction velocity (transactions per day)
        query1 = """
        SELECT 
            AVG(transaction_count) as avg_daily_transactions,
            AVG(transaction_amount) as avg_daily_amount
        FROM (
            SELECT 
                state, year, quarter,
                SUM(transaction_count) / (CASE quarter WHEN 1 THEN 90 WHEN 2 THEN 91 WHEN 3 THEN 92 ELSE 92 END) as transaction_count,
                SUM(transaction_amount) / (CASE quarter WHEN 1 THEN 90 WHEN 2 THEN 91 WHEN 3 THEN 92 ELSE 92 END) as transaction_amount
            FROM aggregated_transaction
            GROUP BY state, year, quarter
        )
        """
        result1 = self.db_ops.execute_query(query1)
        if not result1.empty:
            kpis.update(result1.to_dict('records')[0])
        
        # Market penetration rate
        query2 = """
        SELECT 
            COUNT(DISTINCT state) as total_states,
            AVG(user_penetration) as avg_user_penetration
        FROM (
            SELECT 
                state,
                SUM(registered_users) * 100.0 / 1000000 as user_penetration  -- Assuming 1M potential users per state
            FROM map_user
            GROUP BY state
        )
        """
        result2 = self.db_ops.execute_query(query2)
        if not result2.empty:
            kpis.update(result2.to_dict('records')[0])
        
        # Customer lifetime value proxy
        query3 = """
        SELECT 
            AVG(customer_value) as avg_customer_lifetime_value
        FROM (
            SELECT 
                state,
                SUM(transaction_amount) / NULLIF(SUM(registered_users), 0) as customer_value
            FROM aggregated_transaction at
            JOIN (
                SELECT state, SUM(registered_users) as registered_users
                FROM map_user
                GROUP BY state
            ) mu ON at.state = mu.state
            GROUP BY at.state
        )
        """
        result3 = self.db_ops.execute_query(query3)
        if not result3.empty:
            kpis.update(result3.to_dict('records')[0])
        
        return kpis
    
    def get_competitive_analysis(self) -> pd.DataFrame:
        """Analyze competitive position across states"""
        query = """
        WITH state_metrics AS (
            SELECT 
                state,
                SUM(transaction_amount) as total_amount,
                SUM(transaction_count) as total_transactions,
                COUNT(DISTINCT transaction_type) as transaction_diversity,
                AVG(transaction_amount) as avg_transaction_size
            FROM aggregated_transaction
            GROUP BY state
        ),
        ranked_metrics AS (
            SELECT 
                state,
                total_amount,
                total_transactions,
                transaction_diversity,
                avg_transaction_size,
                RANK() OVER (ORDER BY total_amount DESC) as amount_rank,
                RANK() OVER (ORDER BY total_transactions DESC) as volume_rank,
                RANK() OVER (ORDER BY transaction_diversity DESC) as diversity_rank,
                RANK() OVER (ORDER BY avg_transaction_size DESC) as size_rank
            FROM state_metrics
        )
        SELECT 
            state,
            total_amount,
            total_transactions,
            transaction_diversity,
            avg_transaction_size,
            amount_rank,
            volume_rank,
            diversity_rank,
            size_rank,
            ROUND((amount_rank + volume_rank + diversity_rank + size_rank) / 4.0, 2) as overall_score
        FROM ranked_metrics
        ORDER BY overall_score
        """
        return self.db_ops.execute_query(query)
    
    def get_fraud_risk_indicators(self) -> pd.DataFrame:
        """Identify potential fraud risk indicators (SQLite compatible)"""
        query = """
        WITH transaction_stats AS (
            SELECT 
                state,
                transaction_type,
                AVG(transaction_amount) as avg_amount,
                MAX(transaction_amount) as max_amount,
                MIN(transaction_amount) as min_amount,
                COUNT(*) as transaction_frequency
            FROM aggregated_transaction
            GROUP BY state, transaction_type
        )
        SELECT 
            state,
            transaction_type,
            avg_amount,
            max_amount,
            min_amount,
            transaction_frequency,
            CASE 
                WHEN max_amount > (avg_amount * 3) THEN 'High Risk'
                WHEN max_amount > (avg_amount * 2) THEN 'Medium Risk'
                ELSE 'Low Risk'
            END as risk_level,
            ROUND((max_amount - avg_amount) / NULLIF(avg_amount, 0) * 100, 2) as deviation_percentage
        FROM transaction_stats
        ORDER BY deviation_percentage DESC
        """
        return self.db_ops.execute_query(query)
    
    def close_connection(self):
        """Close database connection"""
        self.db_ops.close()

# Additional utility functions for complex analytics
class AdvancedAnalytics(PhonePeAnalytics):
    """Extended analytics class with advanced statistical methods"""
    
    def get_time_series_forecast_data(self, state: str) -> pd.DataFrame:
        """Prepare data for time series forecasting"""
        query = """
        SELECT 
            year,
            quarter,
            (year - 1) * 4 + quarter as time_period,
            SUM(transaction_amount) as amount,
            SUM(transaction_count) as count
        FROM aggregated_transaction
        WHERE state = ?
        GROUP BY year, quarter
        ORDER BY year, quarter
        """
        return self.db_ops.execute_query(query, (state,))
    
    def get_cohort_analysis_data(self) -> pd.DataFrame:
        """Prepare data for cohort analysis"""
        query = """
        WITH user_first_transaction AS (
            SELECT 
                state,
                MIN(year * 4 + quarter) as first_period
            FROM map_user
            GROUP BY state
        ),
        user_activity AS (
            SELECT 
                mu.state,
                mu.year * 4 + mu.quarter as period,
                uft.first_period,
                SUM(mu.registered_users) as users,
                SUM(mu.app_opens) as opens
            FROM map_user mu
            JOIN user_first_transaction uft ON mu.state = uft.state
            GROUP BY mu.state, mu.year, mu.quarter, uft.first_period
        )
        SELECT 
            first_period,
            period,
            period - first_period as period_number,
            COUNT(DISTINCT state) as cohort_size,
            SUM(users) as total_users,
            SUM(opens) as total_opens
        FROM user_activity
        GROUP BY first_period, period
        ORDER BY first_period, period
        """
        return self.db_ops.execute_query(query)
    
    def get_anomaly_detection_data(self) -> pd.DataFrame:
        """Identify anomalies in transaction patterns (SQLite compatible)"""
        query = """
        WITH monthly_stats AS (
            SELECT 
                state,
                year,
                quarter,
                SUM(transaction_amount) as amount,
                SUM(transaction_count) as count
            FROM aggregated_transaction
            GROUP BY state, year, quarter
        ),
        state_averages AS (
            SELECT 
                state,
                AVG(amount) as avg_amount,
                AVG(count) as avg_count
            FROM monthly_stats
            GROUP BY state
        )
        SELECT 
            ms.state,
            ms.year,
            ms.quarter,
            ms.amount,
            ms.count,
            sa.avg_amount,
            ABS(ms.amount - sa.avg_amount) / NULLIF(sa.avg_amount, 0) * 100 as amount_deviation_percent,
            ABS(ms.count - sa.avg_count) / NULLIF(sa.avg_count, 0) * 100 as count_deviation_percent,
            CASE 
                WHEN ABS(ms.amount - sa.avg_amount) / NULLIF(sa.avg_amount, 0) > 2 THEN 'Anomaly'
                ELSE 'Normal'
            END as anomaly_flag
        FROM monthly_stats ms
        JOIN state_averages sa ON ms.state = sa.state
        ORDER BY amount_deviation_percent DESC
        """
        return self.db_ops.execute_query(query)