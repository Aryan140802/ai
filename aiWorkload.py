import os
import re
import logging
import pymysql
import traceback
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date
import json
from langchain_ollama import OllamaLLM
import time

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root", 
    "password": "root123",
    "database": "EIS_n"
}

# Table schemas for quick reference
TABLES = {
    "dbOpTest_layerdetails": {
        "columns": ["id", "layer_name"],
        "primary_key": "id",
        "searchable": ["layer_name"]
    },
    "dbOpTest_serverdetails": {
        "columns": ["id", "serverIP", "layer_id"],
        "primary_key": "id", 
        "foreign_keys": {"layer_id": "dbOpTest_layerdetails.id"},
        "searchable": ["serverIP"]
    },
    "dbOpTest_brokerdetails": {
        "columns": ["id", "udpPort", "brokerName", "brokerStatus", "server_id"],
        "primary_key": "id",
        "foreign_keys": {"server_id": "dbOpTest_serverdetails.id"},
        "searchable": ["brokerName"],
        "status_field": "brokerStatus"
    },
    "dbOpTest_egdetails": {
        "columns": ["id", "egName", "egStatus", "broker_id"],
        "primary_key": "id",
        "foreign_keys": {"broker_id": "dbOpTest_brokerdetails.id"},
        "searchable": ["egName"],
        "status_field": "egStatus"
    },
    "dbOpTest_servicedetails": {
        "columns": ["id", "serviceName", "serviceStatus", "additionalInstances", 
                   "threadCapicity", "threadInUse", "timeout", "eg_id", "genericName"],
        "primary_key": "id",
        "foreign_keys": {"eg_id": "dbOpTest_egdetails.id"},
        "searchable": ["serviceName", "genericName"],
        "status_field": "serviceStatus"
    },
    "dbOpTest_serviceadditionaldetails": {
        "columns": ["id", "serviceDepDate", "serviceLastEdit", "service_id"],
        "primary_key": "id",
        "foreign_keys": {"service_id": "dbOpTest_servicedetails.id"}
    },
    "dbOpTest_schemas": {
        "columns": ["id", "layer_name", "brokerDetails_id", "egDetails_id", "serviceDetails_id"],
        "primary_key": "id",
        "foreign_keys": {
            "brokerDetails_id": "dbOpTest_brokerdetails.id",
            "egDetails_id": "dbOpTest_egdetails.id", 
            "serviceDetails_id": "dbOpTest_servicedetails.id"
        }
    }
}

# Common JOIN patterns for fast query building
JOIN_PATTERNS = {
    "layer_server": "dbOpTest_layerdetails l JOIN dbOpTest_serverdetails s ON l.id = s.layer_id",
    "server_broker": "dbOpTest_serverdetails s JOIN dbOpTest_brokerdetails b ON s.id = b.server_id",
    "broker_eg": "dbOpTest_brokerdetails b JOIN dbOpTest_egdetails e ON b.id = e.broker_id",
    "eg_service": "dbOpTest_egdetails e JOIN dbOpTest_servicedetails sv ON e.id = sv.eg_id",
    "service_additional": "dbOpTest_servicedetails sv JOIN dbOpTest_serviceadditionaldetails sa ON sv.id = sa.service_id",
    "full_hierarchy": """
        dbOpTest_layerdetails l 
        JOIN dbOpTest_serverdetails s ON l.id = s.layer_id
        JOIN dbOpTest_brokerdetails b ON s.id = b.server_id  
        JOIN dbOpTest_egdetails e ON b.id = e.broker_id
        JOIN dbOpTest_servicedetails sv ON e.id = sv.eg_id
    """
}

class FastOpTestAssistant:
    def __init__(self):
        self.llm = None
        self.connection = None
        self.initialized = False
        
    def initialize(self):
        """Quick initialization"""
        try:
            # Initialize LLM with optimized settings
            self.llm = OllamaLLM(
                model="myllm:latest", 
                temperature=0.0,
                timeout=30  # 30 second timeout
            )
            
            # Database connection
            self.connection = pymysql.connect(
                **DB_CONFIG,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                connect_timeout=10,
                read_timeout=10
            )
            
            self.initialized = True
            return True
            
        except Exception as e:
            print(f"Initialization failed: {e}")
            return False
    
    def create_sql_context(self, question: str) -> str:
        """Create minimal but effective context for SQL generation"""
        context = f"""
You are a SQL expert. Generate ONLY a SELECT query for MySQL database.

DATABASE SCHEMA:
- dbOpTest_layerdetails (id, layer_name)
- dbOpTest_serverdetails (id, serverIP, layer_id) 
- dbOpTest_brokerdetails (id, udpPort, brokerName, brokerStatus, server_id)
- dbOpTest_egdetails (id, egName, egStatus, broker_id)
- dbOpTest_servicedetails (id, serviceName, serviceStatus, additionalInstances, threadCapicity, threadInUse, timeout, eg_id, genericName)
- dbOpTest_serviceadditionaldetails (id, serviceDepDate, serviceLastEdit, service_id)
- dbOpTest_schemas (id, layer_name, brokerDetails_id, egDetails_id, serviceDetails_id)

RELATIONSHIPS:
- layer -> server (layer_id)
- server -> broker (server_id) 
- broker -> eg (broker_id)
- eg -> service (eg_id)
- service -> additional (service_id)

RULES:
1. Status fields: 1=Active, 0=Inactive
2. Use LIKE '%text%' for name searches
3. Use proper JOINs for related data
4. Return only the SQL query, no explanation

QUESTION: {question}

SQL:"""
        return context
    
    def generate_sql(self, question: str) -> str:
        """Generate SQL query using LLM"""
        try:
            context = self.create_sql_context(question)
            raw_sql = self.llm.invoke(context)
            
            # Clean the response
            sql = self.clean_sql(raw_sql)
            return sql
            
        except Exception as e:
            print(f"SQL generation error: {e}")
            return None
    
    def clean_sql(self, raw_sql: str) -> str:
        """Clean and validate SQL"""
        # Remove markdown and extra text
        sql = raw_sql.strip()
        
        # Extract SQL from markdown blocks
        if "```" in sql:
            parts = sql.split("```")
            for part in parts:
                if "SELECT" in part.upper():
                    sql = part.strip()
                    break
        
        # Remove common prefixes
        prefixes = ["sql:", "query:", "here's", "here is"]
        sql_lower = sql.lower()
        for prefix in prefixes:
            if sql_lower.startswith(prefix):
                sql = sql[len(prefix):].strip()
                break
        
        # Ensure it starts with SELECT
        if not sql.upper().strip().startswith("SELECT"):
            return None
            
        # Remove trailing semicolon for consistency
        sql = sql.rstrip(";").strip()
        
        # Basic safety check
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"]
        if any(word in sql.upper() for word in dangerous):
            return None
            
        return sql
    
    def execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL query and return results"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        except Exception as e:
            print(f"Query execution error: {e}")
            return None
    
    def create_response_context(self, question: str, results: List[Dict]) -> str:
        """Create context for natural language response"""
        if not results:
            return f"No results found for: {question}"
            
        context = f"""
Convert this SQL query result to natural language answer.

ORIGINAL QUESTION: {question}

RESULTS: {json.dumps(results, indent=2, default=str)}

RULES:
1. Be concise and direct
2. Format numbers and status properly (1=Active, 0=Inactive)
3. Present data clearly
4. Don't repeat the question
5. If multiple results, summarize key information

NATURAL LANGUAGE RESPONSE:"""
        return context
    
    def generate_response(self, question: str, results: List[Dict]) -> str:
        """Generate natural language response"""
        try:
            context = self.create_response_context(question, results)
            response = self.llm.invoke(context)
            return response.strip()
        except Exception as e:
            print(f"Response generation error: {e}")
            return self.format_results_simple(results)
    
    def format_results_simple(self, results: List[Dict]) -> str:
        """Simple fallback formatting"""
        if not results:
            return "No records found."
            
        if len(results) == 1:
            result = results[0]
            output = "Found 1 record:\n"
            for key, value in result.items():
                if 'status' in key.lower() and isinstance(value, int):
                    value = "Active" if value == 1 else "Inactive"
                output += f"- {key}: {value}\n"
            return output
        else:
            return f"Found {len(results)} records:\n" + json.dumps(results, indent=2, default=str)
    
    def process_question(self, question: str) -> str:
        """Main processing function - optimized for speed"""
        start_time = time.time()
        
        if not self.initialized:
            if not self.initialize():
                return "Failed to initialize system."
        
        try:
            # Step 1: Generate SQL (fast)
            print("Generating SQL query...")
            sql = self.generate_sql(question)
            if not sql:
                return "Could not generate valid SQL query."
            
            print(f"Generated SQL: {sql}")
            
            # Step 2: Execute query (fast)
            print("Executing query...")
            results = self.execute_query(sql)
            if results is None:
                return "Query execution failed."
            
            # Step 3: Generate response (fast)
            print("Generating response...")
            response = self.generate_response(question, results)
            
            end_time = time.time()
            print(f"Total time: {end_time - start_time:.2f} seconds")
            
            return response
            
        except Exception as e:
            return f"Error processing question: {str(e)}"
    
    def quick_query_patterns(self, question: str) -> Optional[str]:
        """Handle common patterns with pre-built queries for speed"""
        q_lower = question.lower()
        
        # Pattern: layer name for server IP
        if "layer name" in q_lower and "server" in q_lower and "ip" in q_lower:
            # Extract IP pattern
            ip_match = re.search(r'\b\d+_\d+\b', question)
            if ip_match:
                ip = ip_match.group()
                return f"SELECT l.layer_name FROM dbOpTest_layerdetails l JOIN dbOpTest_serverdetails s ON l.id = s.layer_id WHERE s.serverIP = '{ip}'"
        
        # Pattern: count active services
        if "count" in q_lower and "active" in q_lower and "service" in q_lower:
            return "SELECT COUNT(*) as active_services FROM dbOpTest_servicedetails WHERE serviceStatus = 1"
        
        # Pattern: all brokers with status
        if "broker" in q_lower and "status" in q_lower:
            return "SELECT brokerName, brokerStatus FROM dbOpTest_brokerdetails"
        
        return None
    
    def fast_process(self, question: str) -> str:
        """Ultra-fast processing with pattern matching"""
        # Try quick patterns first
        quick_sql = self.quick_query_patterns(question)
        if quick_sql:
            results = self.execute_query(quick_sql)
            if results is not None:
                return self.format_results_simple(results)
        
        # Fall back to LLM processing
        return self.process_question(question)

def OpTestMain(query: str) -> str:
    """Main function - optimized for speed"""
    assistant = FastOpTestAssistant()
    try:
        return assistant.fast_process(query)
    finally:
        # Cleanup
        if assistant.connection:
            assistant.connection.close()

# Test function
if __name__ == "__main__":
    test_queries = [
        "tell me the layer name where server ip is 24_191",
        "how many active services are there?", 
        "show me all brokers with their status",
        "find services with high thread usage"
    ]
    
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Query: {query}")
        print('='*50)
        result = OpTestMain(query)
        print(result)
