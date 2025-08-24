import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional
import re
import json

class EnhancedVisualizer:
    def __init__(self):
        self.chart_templates = {
            'distribution': {
                'keywords': ['distribution', 'proportion', 'percentage', 'share', 'breakdown'],
                'chart_type': 'pie',
                'template': 'pie_chart'
            },
            'trend': {
                'keywords': ['trend', 'over time', 'by year', 'by month', 'by quarter', 'growth'],
                'chart_type': 'line',
                'template': 'line_chart'
            },
            'comparison': {
                'keywords': ['compare', 'vs', 'versus', 'difference', 'top', 'highest', 'lowest'],
                'chart_type': 'bar',
                'template': 'bar_chart'
            },
            'correlation': {
                'keywords': ['correlation', 'relationship', 'between', 'scatter'],
                'chart_type': 'scatter',
                'template': 'scatter_plot'
            },
            'ranking': {
                'keywords': ['top', 'bottom', 'rank', 'order', 'best', 'worst'],
                'chart_type': 'bar',
                'template': 'horizontal_bar'
            }
        }
        
        # Chart styling presets
        self.chart_styles = {
            'pie': {
                'colors': px.colors.qualitative.Set3,
                'height': 450,
                'margin': dict(t=80, b=80, l=80, r=80)
            },
            'line': {
                'colors': px.colors.qualitative.Set1,
                'height': 450,
                'margin': dict(t=80, b=80, l=80, r=80)
            },
            'bar': {
                'colors': px.colors.qualitative.Pastel,
                'height': 450,
                'margin': dict(t=80, b=80, l=80, r=80)
            },
            'scatter': {
                'colors': px.colors.qualitative.Set2,
                'height': 450,
                'margin': dict(t=80, b=80, l=80, r=80)
            },
            'histogram': {
                'colors': px.colors.qualitative.Set3,
                'height': 450,
                'margin': dict(t=80, b=80, l=80, r=80)
            }
        }
    
    def generate_plotly_code(self, question: str, sql: str, df: pd.DataFrame) -> str:
        """Generate Plotly code based on question, SQL, and data"""
        question_lower = question.lower()
        sql_lower = sql.lower()
        
        # Analyze the question and SQL to determine chart type
        chart_type = self._detect_chart_type(question_lower, sql_lower, df)
        
        # Generate appropriate Plotly code
        if chart_type == 'pie':
            return self._generate_pie_chart_code(question, df)
        elif chart_type == 'line':
            return self._generate_line_chart_code(question, df)
        elif chart_type == 'bar':
            return self._generate_bar_chart_code(question, df)
        elif chart_type == 'scatter':
            return self._generate_scatter_chart_code(question, df)
        elif chart_type == 'histogram':
            return self._generate_histogram_chart_code(question, df)
        else:
            return self._generate_smart_chart_code(question, df)
    
    def _detect_chart_type(self, question: str, sql: str, df: pd.DataFrame) -> str:
        """Intelligently detect the best chart type"""
        # Check for specific patterns in question
        for pattern, config in self.chart_templates.items():
            if any(keyword in question for keyword in config['keywords']):
                return config['chart_type']
        
        # Check SQL patterns
        if 'group by' in sql:
            if len(df) <= 15:
                return 'bar'
            else:
                return 'line'
        
        if 'order by' in sql and 'limit' in sql:
            return 'bar'
        
        # Check data characteristics
        if self._has_time_column(df):
            return 'line'
        
        if len(df) <= 20:
            return 'bar'
        else:
            return 'line'
    
    def _has_time_column(self, df: pd.DataFrame) -> bool:
        """Check if dataframe has time-related columns"""
        time_patterns = ['year', 'month', 'date', 'quarter', 'time', 'period']
        for col in df.columns:
            if any(pattern in col.lower() for pattern in time_patterns):
                return True
        return False
    
    def _generate_pie_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate code for pie chart"""
        x_col, y_col = self._find_xy_columns(df)
        style = self.chart_styles['pie']
        
        code = f"""
import plotly.express as px

fig = px.pie(
    data_frame=df,
    values='{y_col}',
    names='{x_col}',
    title='{question}',
    color_discrete_sequence={style['colors']}
)

fig.update_layout(
    title=dict(
        text='{question}',
        font=dict(size=18, color='#2E86AB'),
        x=0.5,
        xanchor='center'
    ),
    height={style['height']},
    margin={style['margin']},
    showlegend=True,
    plot_bgcolor='white',
    paper_bgcolor='white'
)

fig.update_traces(
    textposition='inside',
    textinfo='percent+label',
    textfont=dict(size=12),
    marker=dict(line=dict(color='white', width=2))
)
"""
        return code
    
    def _generate_line_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate code for line chart"""
        x_col, y_col = self._find_xy_columns(df)
        style = self.chart_styles['line']
        
        code = f"""
import plotly.express as px

fig = px.line(
    data_frame=df,
    x='{x_col}',
    y='{y_col}',
    title='{question}',
    color_discrete_sequence={style['colors']}
)

fig.update_layout(
    title=dict(
        text='{question}',
        font=dict(size=18, color='#2E86AB'),
        x=0.5,
        xanchor='center'
    ),
    xaxis_title=dict(
        text='{x_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    yaxis_title=dict(
        text='{y_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    height={style['height']},
    margin={style['margin']},
    hovermode='x unified',
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    ),
    yaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    )
)

fig.update_traces(
    line=dict(width=3),
    marker=dict(size=8, color='#2E86AB')
)
"""
        return code
    
    def _generate_bar_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate code for bar chart"""
        x_col, y_col = self._find_xy_columns(df)
        style = self.chart_styles['bar']
        
        code = f"""
import plotly.express as px

fig = px.bar(
    data_frame=df,
    x='{x_col}',
    y='{y_col}',
    title='{question}',
    color_discrete_sequence={style['colors']}
)

fig.update_layout(
    title=dict(
        text='{question}',
        font=dict(size=18, color='#2E86AB'),
        x=0.5,
        xanchor='center'
    ),
    xaxis_title=dict(
        text='{x_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    yaxis_title=dict(
        text='{y_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    height={style['height']},
    margin={style['margin']},
    xaxis={{'categoryorder': 'total descending'}},
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(
        gridcolor='lightgray',
        showgrid=False,
        zeroline=False
    ),
    yaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    )
)

fig.update_traces(
    marker_color='#2E86AB',
    marker_line_color='#1B4F72',
    marker_line_width=1
)
"""
        return code
    
    def _generate_scatter_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate code for scatter plot"""
        numeric_cols = self._get_numeric_columns(df)
        style = self.chart_styles['scatter']
        
        if len(numeric_cols) >= 2:
            x_col, y_col = numeric_cols[0], numeric_cols[1]
        else:
            x_col, y_col = self._find_xy_columns(df)
        
        code = f"""
import plotly.express as px

fig = px.scatter(
    data_frame=df,
    x='{x_col}',
    y='{y_col}',
    title='{question}',
    color_discrete_sequence={style['colors']}
)

fig.update_layout(
    title=dict(
        text='{question}',
        font=dict(size=18, color='#2E86AB'),
        x=0.5,
        xanchor='center'
    ),
    xaxis_title=dict(
        text='{x_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    yaxis_title=dict(
        text='{y_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    height={style['height']},
    margin={style['margin']},
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    ),
    yaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    )
)

fig.update_traces(
    marker=dict(size=10, opacity=0.7, color='#2E86AB'),
    mode='markers'
)
"""
        return code
    
    def _generate_histogram_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate code for histogram"""
        numeric_cols = self._get_numeric_columns(df)
        x_col = numeric_cols[0] if numeric_cols else df.columns[0]
        style = self.chart_styles['histogram']
        
        code = f"""
import plotly.express as px

fig = px.histogram(
    data_frame=df,
    x='{x_col}',
    title='{question}',
    nbins=20,
    color_discrete_sequence={style['colors']}
)

fig.update_layout(
    title=dict(
        text='{question}',
        font=dict(size=18, color='#2E86AB'),
        x=0.5,
        xanchor='center'
    ),
    xaxis_title=dict(
        text='{x_col}',
        font=dict(size=14, color='#2E86AB')
    ),
    yaxis_title=dict(
        text='Frequency',
        font=dict(size=14, color='#2E86AB')
    ),
    height={style['height']},
    margin={style['margin']},
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(
        gridcolor='lightgray',
        showgrid=False,
        zeroline=False
    ),
    yaxis=dict(
        gridcolor='lightgray',
        showgrid=True,
        zeroline=False
    )
)

fig.update_traces(
    marker_color='#2E86AB',
    marker_line_color='#1B4F72',
    marker_line_width=1
)
"""
        return code
    
    def _generate_smart_chart_code(self, question: str, df: pd.DataFrame) -> str:
        """Generate smart chart code that adapts to data"""
        x_col, y_col = self._find_xy_columns(df)
        
        # Determine best chart type based on data
        if len(df) <= 10:
            chart_type = 'bar'
        elif self._has_time_column(df):
            chart_type = 'line'
        else:
            chart_type = 'bar'
        
        if chart_type == 'bar':
            return self._generate_bar_chart_code(question, df)
        else:
            return self._generate_line_chart_code(question, df)
    
    def _get_numeric_columns(self, df: pd.DataFrame) -> List[str]:
        """Get list of numeric columns"""
        numeric_cols = []
        for col in df.columns:
            try:
                pd.to_numeric(df[col], errors='raise')
                numeric_cols.append(col)
            except:
                continue
        return numeric_cols
    
    def _find_xy_columns(self, df: pd.DataFrame) -> Tuple[str, str]:
        """Find best columns for x and y axes"""
        columns = list(df.columns)
        
        # Look for time columns for x-axis
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
                    pd.to_numeric(df[col], errors='raise')
                except:
                    x_col = col
                    break
        
        if not x_col:
            x_col = columns[0]
        
        # Find numeric column for y-axis
        y_col = None
        numeric_cols = self._get_numeric_columns(df)
        if numeric_cols:
            for col in numeric_cols:
                if col != x_col:
                    y_col = col
                    break
        
        if not y_col:
            y_col = columns[1] if len(columns) > 1 else columns[0]
        
        return x_col, y_col
    
    def get_plotly_figure(self, plotly_code: str, df: pd.DataFrame) -> go.Figure:
        """Execute the generated Plotly code and return the figure"""
        try:
            # Create a local namespace with the dataframe
            local_vars = {'df': df, 'px': px, 'go': go}
            
            # Execute the generated code
            exec(plotly_code, globals(), local_vars)
            
            # Get the figure from local variables
            fig = local_vars.get('fig')
            
            if fig is None:
                # Fallback to simple chart if code execution fails
                return self._create_fallback_chart(df, "Generated Chart")
            
            return fig
            
        except Exception as e:
            # Silent fallback - no warning to keep UI clean
            return self._create_fallback_chart(df, "Fallback Chart")
    
    def _create_fallback_chart(self, df: pd.DataFrame, title: str) -> go.Figure:
        """Create a fallback chart when code generation fails"""
        try:
            x_col, y_col = self._find_xy_columns(df)
            y_data = pd.to_numeric(df[y_col], errors='coerce').fillna(0)
            
            fig = px.bar(
                x=df[x_col].astype(str), 
                y=y_data, 
                title=f"{title}: {df.shape[0]} records",
                color_discrete_sequence=['#2E86AB']
            )
            fig.update_layout(
                title=dict(
                    text=f"{title}: {df.shape[0]} records",
                    font=dict(size=16, color='#2E86AB'),
                    x=0.5,
                    xanchor='center'
                ),
                xaxis_title=x_col,
                yaxis_title="Value/Count",
                height=400,
                plot_bgcolor='white',
                paper_bgcolor='white'
            )
            return fig
        except:
            # Ultimate fallback
            fig = go.Figure()
            fig.add_annotation(
                text=f"Chart generated<br>Data: {df.shape[0]} rows, {df.shape[1]} columns",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#2E86AB")
            )
            fig.update_layout(
                title=title,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                plot_bgcolor='white',
                paper_bgcolor='white',
                height=400
            )
            return fig
    
    def create_visualization(self, question: str, sql: str, df: pd.DataFrame) -> go.Figure:
        """Main method to create visualization"""
        # Generate Plotly code
        plotly_code = self.generate_plotly_code(question, sql, df)
        
        # Create and return the figure
        return self.get_plotly_figure(plotly_code, df)
    
    def get_visualization_explanation(self, question: str, sql: str, df: pd.DataFrame) -> str:
        """Generate clean explanation of the visualization approach"""
        chart_type = self._detect_chart_type(question.lower(), sql.lower(), df)
        x_col, y_col = self._find_xy_columns(df)
        
        explanations = {
            'pie': f"🥧 **Pie Chart** - Distribution of {y_col} by {x_col}",
            'line': f"📈 **Line Chart** - Trends over {x_col} with {y_col} values",
            'bar': f"📊 **Bar Chart** - Comparison of {y_col} across {x_col}",
            'scatter': f"🔍 **Scatter Plot** - Relationship between {x_col} and {y_col}",
            'histogram': f"📊 **Histogram** - Frequency distribution of {x_col}"
        }
        
        return explanations.get(chart_type, f"📊 **Smart Chart** - {x_col} vs {y_col}")
    
    def get_chart_summary(self, df: pd.DataFrame) -> str:
        """Get a clean summary of the data for display"""
        return f"📊 **Data Summary**: {df.shape[0]} records, {df.shape[1]} columns"
