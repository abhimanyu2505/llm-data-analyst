import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional
import re

class SmartVisualizer:
    def __init__(self):
        self.chart_types = {
            'line': ['time', 'year', 'month', 'date', 'quarter', 'period'],
            'bar': ['category', 'product', 'region', 'country', 'type', 'group'],
            'pie': ['percentage', 'proportion', 'distribution', 'share'],
            'scatter': ['correlation', 'relationship', 'trend', 'scatter'],
            'histogram': ['distribution', 'frequency', 'histogram'],
            'box': ['quartile', 'boxplot', 'distribution', 'outliers']
        }
    
    def can_visualize(self, sql_query: str, results: pd.DataFrame) -> bool:
        """Always return True - we'll find a way to visualize any data"""
        return True
    
    def detect_chart_type(self, sql_query: str, results: pd.DataFrame) -> str:
        """Intelligently detect the best chart type based on query and data"""
        query_lower = sql_query.lower()
        
        # Check for time-based queries
        if any(time_word in query_lower for time_word in ['year', 'month', 'date', 'quarter', 'time', 'trend']):
            if len(results) > 1:  # Multiple time points
                return 'line'
            else:
                return 'bar'
        
        # Check for grouping queries
        if 'group by' in query_lower:
            if len(results) <= 15:  # Good for bar charts
                return 'bar'
            else:
                return 'line'
        
        # Check for comparison queries
        if any(word in query_lower for word in ['compare', 'vs', 'versus', 'difference']):
            return 'bar'
        
        # Check for distribution queries
        if any(word in query_lower for word in ['distribution', 'frequency', 'histogram']):
            return 'histogram'
        
        # Check for correlation queries
        if any(word in query_lower for word in ['correlation', 'relationship', 'scatter']):
            return 'scatter'
        
        # Default to bar chart for grouped data
        if len(results) <= 20:
            return 'bar'
        else:
            return 'line'
    
    def create_visualization(self, sql_query: str, results: pd.DataFrame, question: str) -> Optional[go.Figure]:
        """Create an appropriate visualization for ANY data"""
        if results.empty or len(results) == 0:
            return self._create_empty_chart(question)
        
        chart_type = self.detect_chart_type(sql_query, results)
        
        try:
            if chart_type == 'line':
                return self._create_line_chart(results, question)
            elif chart_type == 'bar':
                return self._create_bar_chart(results, question)
            elif chart_type == 'pie':
                return self._create_pie_chart(results, question)
            elif chart_type == 'scatter':
                return self._create_scatter_chart(results, question)
            elif chart_type == 'histogram':
                return self._create_histogram(results, question)
            else:
                return self._create_bar_chart(results, question)  # Default fallback
        except Exception as e:
            # If any chart fails, try to create a simple bar chart
            try:
                return self._create_simple_bar_chart(results, question)
            except:
                return self._create_empty_chart(question)
    
    def _create_empty_chart(self, question: str) -> go.Figure:
        """Create a chart for empty results"""
        fig = go.Figure()
        fig.add_annotation(
            text="No data to visualize",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20, color="gray")
        )
        fig.update_layout(
            title=f"Visualization: {question}",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='white'
        )
        return fig
    
    def _create_simple_bar_chart(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a simple bar chart that works with any data"""
        try:
            # Convert all data to strings for x-axis
            x_col = results.columns[0]
            y_data = []
            
            # Try to extract numerical values from any column
            for col in results.columns:
                try:
                    # Convert to numeric, ignoring errors
                    numeric_data = pd.to_numeric(results[col], errors='coerce')
                    if not numeric_data.isna().all():
                        y_data = numeric_data.fillna(0)
                        break
                except:
                    continue
            
            if len(y_data) == 0:
                # If no numeric data, create a count chart
                y_data = [1] * len(results)
            
            fig = px.bar(x=results[x_col].astype(str), y=y_data, title=f"Data Overview: {question}")
            fig.update_layout(
                xaxis_title=x_col,
                yaxis_title="Value/Count",
                xaxis={'categoryorder': 'total descending'}
            )
            return fig
        except:
            return self._create_empty_chart(question)
    
    def _create_line_chart(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a line chart for time series or sequential data"""
        try:
            x_col, y_col = self._find_xy_columns(results)
            
            # Ensure y_col has numeric data
            y_data = pd.to_numeric(results[y_col], errors='coerce').fillna(0)
            
            fig = px.line(x=results[x_col], y=y_data, title=f"Trend Analysis: {question}")
            fig.update_layout(
                xaxis_title=x_col,
                yaxis_title=y_col,
                hovermode='x unified'
            )
            return fig
        except:
            return self._create_simple_bar_chart(results, question)
    
    def _create_bar_chart(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a bar chart for categorical comparisons"""
        try:
            x_col, y_col = self._find_xy_columns(results)
            
            # Ensure y_col has numeric data
            y_data = pd.to_numeric(results[y_col], errors='coerce').fillna(0)
            
            fig = px.bar(x=results[x_col].astype(str), y=y_data, title=f"Comparison: {question}")
            fig.update_layout(
                xaxis_title=x_col,
                yaxis_title=y_col,
                xaxis={'categoryorder': 'total descending'}
            )
            return fig
        except:
            return self._create_simple_bar_chart(results, question)
    
    def _create_pie_chart(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a pie chart for proportions"""
        try:
            x_col, y_col = self._find_xy_columns(results)
            
            # Ensure y_col has numeric data
            y_data = pd.to_numeric(results[y_col], errors='coerce').fillna(0)
            
            fig = px.pie(values=y_data, names=results[x_col].astype(str), title=f"Distribution: {question}")
            return fig
        except:
            return self._create_simple_bar_chart(results, question)
    
    def _create_scatter_chart(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a scatter plot for correlations"""
        try:
            numeric_cols = []
            for col in results.columns:
                try:
                    numeric_data = pd.to_numeric(results[col], errors='coerce')
                    if not numeric_data.isna().all():
                        numeric_cols.append(col)
                except:
                    continue
            
            if len(numeric_cols) >= 2:
                x_data = pd.to_numeric(results[numeric_cols[0]], errors='coerce').fillna(0)
                y_data = pd.to_numeric(results[numeric_cols[1]], errors='coerce').fillna(0)
                
                fig = px.scatter(x=x_data, y=y_data, title=f"Correlation: {question}")
                fig.update_layout(
                    xaxis_title=numeric_cols[0],
                    yaxis_title=numeric_cols[1]
                )
                return fig
            else:
                return self._create_simple_bar_chart(results, question)
        except:
            return self._create_simple_bar_chart(results, question)
    
    def _create_histogram(self, results: pd.DataFrame, question: str) -> go.Figure:
        """Create a histogram for distributions"""
        try:
            numeric_cols = []
            for col in results.columns:
                try:
                    numeric_data = pd.to_numeric(results[col], errors='coerce')
                    if not numeric_data.isna().all():
                        numeric_cols.append(col)
                except:
                    continue
            
            if len(numeric_cols) > 0:
                x_data = pd.to_numeric(results[numeric_cols[0]], errors='coerce').fillna(0)
                fig = px.histogram(x=x_data, title=f"Distribution: {question}")
                fig.update_layout(xaxis_title=numeric_cols[0])
                return fig
            else:
                return self._create_simple_bar_chart(results, question)
        except:
            return self._create_simple_bar_chart(results, question)
    
    def _find_xy_columns(self, results: pd.DataFrame) -> Tuple[str, str]:
        """Find the best columns for x and y axes - more flexible approach"""
        columns = list(results.columns)
        
        # Check for time/date columns
        time_patterns = ['year', 'month', 'date', 'quarter', 'time', 'period']
        x_col = None
        for col in columns:
            if any(pattern in col.lower() for pattern in time_patterns):
                x_col = col
                break
        
        # If no time column, use first categorical column
        if not x_col:
            for col in columns:
                try:
                    # Try to convert to numeric
                    pd.to_numeric(results[col], errors='raise')
                    # If successful, this is numeric, skip it for x-axis
                except:
                    # If conversion fails, this is categorical, use it for x-axis
                    x_col = col
                    break
        
        # If still no x_col, use first column
        if not x_col:
            x_col = columns[0]
        
        # Find numerical column for y-axis
        y_col = None
        for col in columns:
            if col != x_col:  # Don't use the same column for both axes
                try:
                    numeric_data = pd.to_numeric(results[col], errors='coerce')
                    if not numeric_data.isna().all():
                        y_col = col
                        break
                except:
                    continue
        
        # If no numeric column found, use any other column
        if not y_col:
            for col in columns:
                if col != x_col:
                    y_col = col
                    break
        
        # If still no y_col, use x_col (will create count chart)
        if not y_col:
            y_col = x_col
        
        return x_col, y_col
    
    def get_visualization_explanation(self, sql_query: str, results: pd.DataFrame) -> str:
        """Generate explanation of the visualization"""
        if results.empty or len(results) == 0:
            return "📊 **Empty Results** - No data to visualize"
        
        chart_type = self.detect_chart_type(sql_query, results)
        x_col, y_col = self._find_xy_columns(results)
        
        explanations = {
            'line': f"📈 **Line Chart** - Showing trends over {x_col} with {y_col} values",
            'bar': f"📊 **Bar Chart** - Comparing {y_col} across different {x_col} categories",
            'pie': f"🥧 **Pie Chart** - Showing distribution of {y_col} by {x_col}",
            'scatter': f"🔍 **Scatter Plot** - Exploring relationship between {x_col} and {y_col}",
            'histogram': f"📊 **Histogram** - Showing distribution of {x_col} values"
        }
        
        return explanations.get(chart_type, f"📊 **Chart Generated** - Visualizing {x_col} vs {y_col}")
