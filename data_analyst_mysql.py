import pandas as pd
import mysql.connector
import requests
from typing import Dict, Any
import os
import json
import re
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class DataAnalystAssistant:
    def __init__(self, ollama_url: str = "http://localhost:11434", mysql_config: Dict = None):
        self.ollama_url = ollama_url
        self.mysql_config = mysql_config
        self.db_connection = None
        self.engine = None
        self.tables = {}
        
        if mysql_config:
            self.connect_mysql()
    
    def connect_mysql(self):
        try:
            self.db_connection = mysql.connector.connect(**self.mysql_config)
            
            # Create SQLAlchemy engine for pandas
            user = quote_plus(str(self.mysql_config['user']))
            password = quote_plus(str(self.mysql_config['password']))
            host = self.mysql_config['host']
            port = self.mysql_config.get('port', 3306)
            database = self.mysql_config['database']
            
            connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(connection_string)
            
            self.load_existing_tables()
            return "Connected to MySQL successfully!"
        except Exception as e:
            raise Exception(f"Failed to connect to MySQL: {str(e)}")
    
    def load_existing_tables(self):
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns_info = cursor.fetchall()
                columns = [col[0] for col in columns_info]
                
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 3")
                sample_rows = cursor.fetchall()
                sample_data = [dict(zip(columns, row)) for row in sample_rows]
                
                self.tables[table_name] = {
                    'columns': columns,
                    'sample_data': sample_data
                }
            
            cursor.close()
        except Exception as e:
            print(f"Error loading existing tables: {e}")
    
    def load_file(self, file_path: str, table_name: str = None) -> str:
        if not table_name:
            table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # If MySQL connected, use it; otherwise use in-memory SQLite
        if self.engine:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            storage = "MySQL"
        else:
            import sqlite3
            if not hasattr(self, 'sqlite_conn'):
                self.sqlite_conn = sqlite3.connect(':memory:', check_same_thread=False)
            df.to_sql(table_name, self.sqlite_conn, if_exists='replace', index=False)
            storage = "in-memory SQLite"
        
        self.tables[table_name] = {
            'columns': list(df.columns),
            'sample_data': df.head(3).to_dict('records')
        }
        
        return f"Loaded {len(df)} rows into {storage} table '{table_name}'"
    
    def get_available_tables(self) -> list:
        return list(self.tables.keys())
    
    def get_schema_context(self) -> str:
        context = "DATABASE SCHEMA:\n"
        for table_name, info in self.tables.items():
            context += f"\nTABLE: {table_name}\n"
            context += f"COLUMNS: {', '.join(info['columns'])}\n"
            context += f"SAMPLE: {info['sample_data'][0] if info['sample_data'] else 'No data'}\n"
        return context
    
    def quote_column_names(self, sql: str) -> str:
        all_columns = []
        for table_info in self.tables.values():
            all_columns.extend(table_info['columns'])
        
        all_columns = sorted(set(all_columns), key=len, reverse=True)
        
        for col in all_columns:
            if ' ' in col or any(char in col for char in ['-', '.', '(', ')']):
                if f'`{col}`' not in sql:
                    sql = re.sub(rf'\b{re.escape(col)}\b(?!`)', f'`{col}`', sql, flags=re.IGNORECASE)
        
        return sql
    
    def clean_sql(self, sql: str) -> str:
        sql = re.sub(r'```sql\n?|```\n?|SQL:|Query:', '', sql, flags=re.IGNORECASE)
        sql = sql.strip()
        
        sql_pattern = r'(SELECT.*?(?:;|$)|INSERT.*?(?:;|$)|UPDATE.*?(?:;|$)|DELETE.*?(?:;|$))'
        match = re.search(sql_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            cleaned = match.group(1).strip().rstrip(';')
            return self.quote_column_names(cleaned)
        
        lines = []
        for line in sql.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('--'):
                if any(word in line.lower() for word in ['to find', 'you would', "here's how", 'this query']):
                    continue
                if line.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE')):
                    lines.append(line)
        
        cleaned = ' '.join(lines).strip()
        return self.quote_column_names(cleaned)
    
    def detect_language(self, text: str) -> str:
        # Enhanced language detection
        english_words = ['what', 'show', 'get', 'find', 'list', 'count', 'sum', 'average', 'top', 'highest', 'lowest', 'how', 'which', 'where', 'when', 'why', 'the', 'and', 'or', 'of', 'in', 'to', 'for']
        english_count = sum(1 for word in english_words if word in text.lower())
        
        # If more than 2 English words found, consider it English
        if english_count >= 2:
            return 'english'
        
        # Check for non-Latin characters (indicates non-English)
        has_non_latin = any(ord(char) > 127 for char in text)
        if has_non_latin:
            return 'other'
        
        # Default to English if uncertain
        return 'english'
    
    def translate_to_english(self, text: str) -> tuple:
        try:
            if self.detect_language(text) == 'english':
                return text, 'english'
            
            # Enhanced translation to English with better context
            translate_prompt = f"""Translate this text to clear, natural English. Preserve the original meaning and intent exactly.

Text: {text}

Provide only the English translation:"""
            
            response = requests.post(f"{self.ollama_url}/api/generate", json={
                "model": "llama3",
                "prompt": translate_prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 100}
            })
            
            if response.status_code == 200:
                result = response.json()
                if result and "response" in result and result["response"]:
                    translated = result["response"].strip()
                    # Clean up common prefixes
                    if translated.lower().startswith(('english translation:', 'translation:', 'english:')):
                        translated = translated.split(':', 1)[1].strip()
                    return translated, 'other'
        except Exception:
            pass
        
        return text, 'english'
    
    def translate_from_english(self, english_text: str, original_text: str) -> str:
        try:
            # Much simpler and direct prompt
            translate_prompt = f"Translate '{english_text}' to the same language as '{original_text}'"
            
            response = requests.post(f"{self.ollama_url}/api/generate", json={
                "model": "llama3",
                "prompt": translate_prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 100}
            })
            
            if response.status_code == 200:
                result = response.json()
                if result and "response" in result and result["response"]:
                    translated = result["response"].strip()
                    # Don't return if it's the same as original question
                    if translated != original_text and len(translated) > 10:
                        return translated
                    
        except Exception as e:
            pass
        return english_text
    
    def nl_to_sql(self, question: str) -> str:
        english_question, _ = self.translate_to_english(question)
        
        schema = self.get_schema_context()
        table_names = list(self.tables.keys())
        
        if not table_names:
            raise Exception("No tables loaded. Please upload a file first.")
        
        prompt = f"""{schema}

Generate ONLY a SQL query for this question: {english_question}

IMPORTANT:
- Return ONLY the SQL query
- No explanations or text
- Start with SELECT
- Use exact table/column names from schema
- Put column names with spaces in backticks like `Invoice Number`

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
        try:
            if self.engine:
                return pd.read_sql_query(sql_query, self.engine)
            elif hasattr(self, 'sqlite_conn'):
                return pd.read_sql_query(sql_query, self.sqlite_conn)
            else:
                raise Exception("No database connection available")
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
            english_question, original_language = self.translate_to_english(question)
            
            sql_query = self.nl_to_sql(question)
            results = self.execute_query(sql_query)
            english_insights = self.generate_insights(english_question, sql_query, results)
            
            # Always translate insights back if it was a non-English question
            if original_language == 'other':
                insights = self.translate_from_english(english_insights, question)
                
                # If translation failed, try simpler approach
                if insights == english_insights or self.detect_language(insights) == 'english':
                    simple_prompt = f"Convert this English text to the same language as '{question}': {english_insights}"
                    try:
                        response = requests.post(f"{self.ollama_url}/api/generate", json={
                            "model": "llama3",
                            "prompt": simple_prompt,
                            "stream": False,
                            "options": {"temperature": 0.2, "num_predict": 200}
                        })
                        if response.status_code == 200:
                            result = response.json()
                            if result and "response" in result:
                                insights = result["response"].strip()
                    except Exception:
                        pass
            else:
                insights = english_insights
            
            return {
                'question': question,
                'english_question': english_question if original_language == 'other' else None,
                'was_translated': original_language == 'other',
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
    
    def load_excel(self, file_path: str, table_name: str = None) -> str:
        return self.load_file(file_path, table_name)