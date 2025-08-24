import pandas as pd
import mysql.connector
import requests
from typing import Dict, Any
import os
import json
import re

class DataAnalystAssistant:
    def __init__(self, ollama_url: str = "http://localhost:11434", mysql_config: Dict = None):
        self.ollama_url = ollama_url
        self.mysql_config = mysql_config
        self.db_connection = None
        self.tables = {}
        
        if mysql_config:
            self.connect_mysql()
        
    def connect_mysql(self):
        try:
            self.db_connection = mysql.connector.connect(**self.mysql_config)
            return "Connected to MySQL successfully!"
        except Exception as e:
            raise Exception(f"Failed to connect to MySQL: {str(e)}")
    
    def load_file(self, file_path: str, table_name: str = None) -> str:
        if not self.db_connection:
            raise Exception("No database connection. Please connect to MySQL first.")
            
        if not table_name:
            table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
            
        # Clean column names for MySQL
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
        
        # Create table in MySQL
        cursor = self.db_connection.cursor()
        
        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # Create table with proper data types
        columns_def = []
        for col in df.columns:
            if df[col].dtype == 'object':
                columns_def.append(f"`{col}` TEXT")
            elif df[col].dtype in ['int64', 'int32']:
                columns_def.append(f"`{col}` INT")
            elif df[col].dtype in ['float64', 'float32']:
                columns_def.append(f"`{col}` DECIMAL(15,2)")
            else:
                columns_def.append(f"`{col}` TEXT")
        
        create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns_def)})"
        cursor.execute(create_table_sql)
        
        # Insert data
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
        
        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))
        
        self.db_connection.commit()
        cursor.close()
        
        self.tables[table_name] = {
            'columns': list(df.columns),
            'sample_data': df.head(3).to_dict('records')
        }
        
        return f"Loaded {len(df)} rows into MySQL table '{table_name}'"
    
    def get_schema_context(self) -> str:
        context = "DATABASE SCHEMA:\n"
        for table_name, info in self.tables.items():
            context += f"\nTABLE: {table_name}\n"
            context += f"COLUMNS: {', '.join(info['columns'])}\n"
            context += f"SAMPLE: {info['sample_data'][0] if info['sample_data'] else 'No data'}\n"
        return context
    
    def clean_sql(self, sql: str) -> str:
        sql = re.sub(r'```sql\n?|```\n?|SQL:|Query:', '', sql, flags=re.IGNORECASE)
        sql = sql.strip()
        
        sql_pattern = r'(SELECT.*?(?:;|$)|INSERT.*?(?:;|$)|UPDATE.*?(?:;|$)|DELETE.*?(?:;|$))'
        match = re.search(sql_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            return match.group(1).strip().rstrip(';')
        
        lines = []
        for line in sql.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('--'):
                if any(word in line.lower() for word in ['to find', 'you would', 'here\'s how', 'this query']):
                    continue
                if line.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE')):
                    lines.append(line)
        
        return ' '.join(lines).strip()
    
    def nl_to_sql(self, question: str) -> str:
        schema = self.get_schema_context()
        table_names = list(self.tables.keys())
        
        if not table_names:
            raise Exception("No tables loaded. Please upload a file first.")
        
        prompt = f"""{schema}

Generate ONLY a SQL query for this question: {question}

IMPORTANT:
- Return ONLY the SQL query
- No explanations or text
- Start with SELECT
- Use exact table/column names from schema
- Use backticks for table/column names

SQL:"""
        
        try:
            response = requests.post(f"{self.ollama_url}/api/generate", json={
                "model": "llama3",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0, "num_predict": 50}
            })
            
            if response.status_code == 200:
                result = response.json()
                if result and "response" in result and result["response"]:
                    return self.clean_sql(result["response"])
        except Exception:
            pass
        
        if 'count' in question.lower():
            return f"SELECT COUNT(*) FROM `{table_names[0]}`"
        elif any(word in question.lower() for word in ['top', 'highest', 'max']):
            return f"SELECT * FROM `{table_names[0]}` LIMIT 10"
        else:
            return f"SELECT * FROM `{table_names[0]}` LIMIT 5"
    
    def execute_query(self, sql_query: str) -> pd.DataFrame:
        if not self.db_connection:
            raise Exception("No database connection")
            
        try:
            return pd.read_sql(sql_query, self.db_connection)
        except Exception as e:
            raise Exception(f"Query failed: {str(e)}")
    
    def generate_insights(self, question: str, query: str, results: pd.DataFrame) -> str:
        if len(results) == 0:
            return "No results found for your query."
        
        summary = f"Query returned {len(results)} rows."
        if len(results) > 0:
            summary += f" Sample: {results.head(2).to_dict('records')}"
        
        prompt = f"Question: {question}\nResults: {summary}\n\nAnswer the question naturally based on the results. Be conversational and helpful."
        
        try:
            response = requests.post(f"{self.ollama_url}/api/generate", json={
                "model": "llama3",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 80}
            })
            
            if response.status_code == 200:
                result = response.json()
                if result and "response" in result and result["response"]:
                    return result["response"].strip()
        except Exception:
            pass
        
        return f"Found {len(results)} results for your query."
    
    def analyze(self, question: str) -> Dict[str, Any]:
        try:
            sql_query = self.nl_to_sql(question)
            results = self.execute_query(sql_query)
            insights = self.generate_insights(question, sql_query, results)
            
            return {
                'question': question,
                'sql_query': sql_query,
                'results': results.to_dict('records'),
                'insights': insights,
                'success': True
            }
            
        except Exception as e:
            return {
                'question': question,
                'error': str(e),
                'success': False
            }