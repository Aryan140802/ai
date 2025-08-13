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

# Updated table schemas with sample data context
TABLES = {
    "dbOpTest_layerdetails": {
        "columns": ["id", "layer_name"],
        "primary_key": "id",
        "searchable": ["layer_name"],
        "description": "Layer details - contains AADHAR_EXP, AADHAR_SYS, ACCOUNT_EXP, ACCOUNT_SYS, CUSTOMER_EXP",
        "sample_data": "AADHAR_EXP, AADHAR_SYS, ACCOUNT_EXP"
    },
    "dbOpTest_serverdetails": {
        "columns": ["id", "serverIP", "layer_id"],
        "primary_key": "id", 
        "foreign_keys": {"layer_id": "dbOpTest_layerdetails.id"},
        "searchable": ["serverIP"],
        "description": "Server details with IP addresses like 25_190, 25_191, 24_191",
        "sample_data": "25_190, 25_191, 24_191"
    },
    "dbOpTest_brokerdetails": {
        "columns": ["id", "udpPort", "brokerName", "brokerStatus", "server_id"],
        "primary_key": "id",
        "foreign_keys": {"server_id": "dbOpTest_serverdetails.id"},
        "searchable": ["brokerName"],
        "status_field": "brokerStatus",
        "description": "Broker details - brokerName like AADHAR_EXP, port 4417, status 1=Active/0=Inactive",
        "sample_data": "AADHAR_EXP brokers on port 4417"
    },
    "dbOpTest_egdetails": {
        "columns": ["id", "egName", "egStatus", "broker_id"],
        "primary_key": "id",
        "foreign_keys": {"broker_id": "dbOpTest_brokerdetails.id"},
        "searchable": ["egName"],
        "status_field": "egStatus",
        "description": "EG (Execution Group) details - egName like AADHAR_EXP_01, status 1=Active/0=Inactive",
        "sample_data": "AADHAR_EXP_01"
    },
    "dbOpTest_servicedetails": {
        "columns": ["id", "serviceName", "serviceStatus", "additionalInstances", 
                   "threadCapicity", "threadInUse", "timeout", "eg_id", "genericName"],
        "primary_key": "id",
        "foreign_keys": {"eg_id": "dbOpTest_egdetails.id"},
        "searchable": ["serviceName", "genericName"],
        "status_field": "serviceStatus",
        "description": "Service details - serviceName like Dynatrace_Logger, panEnquiryPassThrough_expDs, thread info, status 1=Active/0=Inactive",
        "sample_data": "Dynatrace_Logger, panEnquiryPassThrough_expDs, panValidationEnquiryDotNet_expDS"
    },
    "dbOpTest_serviceadditionaldetails": {
        "columns": ["id", "serviceDepDate", "serviceLastEdit", "service_id"],
        "primary_key": "id",
        "foreign_keys": {"service_id": "dbOpTest_servicedetails.id"},
        "description": "Additional service info - deployment dates, last edit dates",
        "sample_data": "Dates like 2022-08-04_11:59:33, Last edited: '2024-01-13 02:44:28'"
    },
    "dbOpTest_schemas": {
        "columns": ["id", "layer_name", "brokerDetails_id", "egDetails_id", "serviceDetails_id"],
        "primary_key": "id",
        "foreign_keys": {
            "brokerDetails_id": "dbOpTest_brokerdetails.id",
            "egDetails_id": "dbOpTest_egdetails.id", 
            "serviceDetails_id": "dbOpTest_servicedetails.id"
        },
        "description": "Schema mapping between layers and components"
    }
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
                timeout=30
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
    
    def analyze_question(self, question: str) -> Dict[str, Any]:
        """Analyze what user is asking for to determine tables needed"""
        q_lower = question.lower()
        
        analysis = {
            "tables_needed": [],
            "query_type": "unknown",
            "search_terms": [],
            "multiple_queries": False
        }
        
        # Detect what tables are needed
        if any(word in q_lower for word in ["layer", "layers"]):
            analysis["tables_needed"].append("dbOpTest_layerdetails")
            
        if any(word in q_lower for word in ["server", "servers", "ip"]):
            analysis["tables_needed"].append("dbOpTest_serverdetails")
            
        if any(word in q_lower for word in ["broker", "brokers"]):
            analysis["tables_needed"].append("dbOpTest_brokerdetails")
            
        if any(word in q_lower for word in ["eg", "execution group", "edge"]):
            analysis["tables_needed"].append("dbOpTest_egdetails")
            
        if any(word in q_lower for word in ["service", "services"]):
            analysis["tables_needed"].append("dbOpTest_servicedetails")
            
        if any(word in q_lower for word in ["deployment", "deployed", "additional", "date"]):
            analysis["tables_needed"].append("dbOpTest_serviceadditionaldetails")
        
        # Detect query type
        if any(word in q_lower for word in ["count", "how many", "total", "number"]):
            analysis["query_type"] = "count"
        elif any(word in q_lower for word in ["list", "show", "display", "get", "find", "all"]):
            analysis["query_type"] = "list"
        elif any(word in q_lower for word in ["status", "active", "inactive"]):
            analysis["query_type"] = "status"
        
        # Check for multiple queries (and, also, plus)
        if any(word in q_lower for word in [" and ", " also ", " plus ", ","]):
            analysis["multiple_queries"] = True
        
        # Extract search terms
        # IP patterns
        ip_match = re.findall(r'\b\d+_\d+\b', question)
        analysis["search_terms"].extend(ip_match)
        
        # Names in quotes
        quoted = re.findall(r'["\']([^"\']+)["\']', question)
        analysis["search_terms"].extend(quoted)
        
        return analysis
    
    def create_sql_context(self, question: str, analysis: Dict[str, Any]) -> str:
        """Create comprehensive context for SQL generation"""
        
        # Build table info for relevant tables
        relevant_tables = analysis.get("tables_needed", [])
        if not relevant_tables:
            # If no specific tables detected, include all main tables
            relevant_tables = ["dbOpTest_layerdetails", "dbOpTest_serverdetails", 
                             "dbOpTest_brokerdetails", "dbOpTest_egdetails", "dbOpTest_servicedetails"]
        
        table_info = ""
        for table in relevant_tables:
            if table in TABLES:
                schema = TABLES[table]
                table_info += f"\n{table}:\n"
                table_info += f"  Columns: {', '.join(schema['columns'])}\n"
                table_info += f"  Description: {schema.get('description', '')}\n"
                if 'sample_data' in schema:
                    table_info += f"  Sample data: {schema['sample_data']}\n"
                if 'foreign_keys' in schema:
                    table_info += f"  Relationships: {schema['foreign_keys']}\n"
        
        context = f"""
You are a SQL expert. Generate ONLY a SELECT query for MySQL database.

RELEVANT TABLES FOR THIS QUESTION:{table_info}

HIERARCHY: layer -> server -> broker -> eg -> service -> service_additional

RELATIONSHIPS:
- dbOpTest_serverdetails.layer_id -> dbOpTest_layerdetails.id
- dbOpTest_brokerdetails.server_id -> dbOpTest_serverdetails.id  
- dbOpTest_egdetails.broker_id -> dbOpTest_brokerdetails.id
- dbOpTest_servicedetails.eg_id -> dbOpTest_egdetails.id
- dbOpTest_serviceadditionaldetails.service_id -> dbOpTest_servicedetails.id

IMPORTANT RULES:
1. Status fields: 1=Active, 0=Inactive  
2. Use LIKE '%text%' for name searches
3. For COUNT queries, count from the main table (don't use JOINs unless specifically needed)
4. Use proper JOINs only when querying across hierarchy levels
5. If asking about "deployed services", count from dbOpTest_servicedetails table
6. Return ONLY the SQL query, no explanation or formatting

QUESTION ANALYSIS: {analysis}

QUESTION: {question}

SQL:"""
        return context
    
    def generate_sql(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate SQL query using LLM with analysis"""
        try:
            analysis = self.analyze_question(question)
            context = self.create_sql_context(question, analysis)
            raw_sql = self.llm.invoke(context)
            
            sql = self.clean_sql(raw_sql)
            return sql, analysis
            
        except Exception as e:
            print(f"SQL generation error: {e}")
            return None, {}
    
    def clean_sql(self, raw_sql: str) -> str:
        """Clean and validate SQL"""
        sql = raw_sql.strip()
        
        # Extract SQL from markdown blocks
        if "```" in sql:
            parts = sql.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("sql"):
                    part = part[3:].strip()
                if "SELECT" in part.upper():
                    sql = part.strip()
                    break
        
        # Remove common prefixes
        prefixes = ["sql:", "query:", "here's the sql:", "here is the sql:", "select query:"]
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
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"]
        if any(word in sql.upper() for word in dangerous):
            return None
            
        return sql
    
    def execute_query(self, sql: str) -> Tuple[List[Dict], str]:
        """Execute SQL query and return results with execution info"""
        try:
            start_time = time.time()
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                exec_time = time.time() - start_time
                
            exec_info = f"Query executed in {exec_time:.3f}s, returned {len(results)} rows"
            return results, exec_info
            
        except Exception as e:
            error_info = f"Query execution failed: {str(e)}"
            print(error_info)
            return None, error_info
    
    def quick_query_patterns(self, question: str) -> Optional[Tuple[str, str]]:
        """Handle common patterns with pre-built queries for speed"""
        q_lower = question.lower()
        
        # Pattern: layer name for server IP
        if "layer name" in q_lower and "server" in q_lower and "ip" in q_lower:
            ip_match = re.search(r'\b\d+_\d+\b', question)
            if ip_match:
                ip = ip_match.group()
                sql = f"SELECT l.layer_name FROM dbOpTest_layerdetails l JOIN dbOpTest_serverdetails s ON l.id = s.layer_id WHERE s.serverIP = '{ip}'"
                return sql, "Quick pattern: layer name by server IP"
        
        # Pattern: count services (be more specific about what to count)
        if any(phrase in q_lower for phrase in ["how many services", "count services", "total services", "number of services"]):
            if "active" in q_lower:
                sql = "SELECT COUNT(*) as active_services FROM dbOpTest_servicedetails WHERE serviceStatus = 1"
                return sql, "Quick pattern: count active services"
            elif "deployed" in q_lower:
                # Count all services that have deployment information
                sql = "SELECT COUNT(*) as deployed_services FROM dbOpTest_servicedetails"
                return sql, "Quick pattern: count deployed services"
            else:
                # Default to all services
                sql = "SELECT COUNT(*) as total_services FROM dbOpTest_servicedetails"
                return sql, "Quick pattern: count all services"
        
        # Pattern: all brokers with status  
        if "broker" in q_lower and ("status" in q_lower or "all" in q_lower):
            sql = "SELECT brokerName, CASE WHEN brokerStatus = 1 THEN 'Active' ELSE 'Inactive' END as status FROM dbOpTest_brokerdetails"
            return sql, "Quick pattern: all brokers with status"
        
        return None, None
    
    def create_response_context(self, question: str, results: List[Dict], sql: str, exec_info: str) -> str:
        """Create context for natural language response"""
        if not results:
            return f"No results found for: {question}"
            
        # Limit results for response generation to avoid token limits
        display_results = results[:10] if len(results) > 10 else results
        
        context = f"""
Convert this SQL query result to a natural language answer.

ORIGINAL QUESTION: {question}
SQL EXECUTED: {sql}
EXECUTION INFO: {exec_info}

RESULTS (showing {len(display_results)} of {len(results)} total):
{json.dumps(display_results, indent=2, default=str)}

FORMATTING RULES:
1. Be direct and concise
2. Convert status numbers: 1=Active, 0=Inactive  
3. If count query, state the number clearly
4. If list query, present data in readable format
5. Don't repeat the question
6. Don't mention SQL or technical details

NATURAL LANGUAGE RESPONSE:"""
        return context
    
    def generate_response(self, question: str, results: List[Dict], sql: str, exec_info: str) -> str:
        """Generate natural language response"""
        try:
            context = self.create_response_context(question, results, sql, exec_info)
            response = self.llm.invoke(context)
            return response.strip()
        except Exception as e:
            print(f"Response generation error: {e}")
            return self.format_results_simple(results)
    
    def format_results_simple(self, results: List[Dict]) -> str:
        """Simple fallback formatting"""
        if not results:
            return "No records found."
            
        if len(results) == 1 and len(results[0]) == 1:
            # Single value result (like COUNT)
            value = list(results[0].values())[0]
            return f"Result: {value}"
            
        if len(results) == 1:
            result = results[0]
            output = "Found 1 record:\n"
            for key, value in result.items():
                if 'status' in key.lower() and isinstance(value, int):
                    value = "Active" if value == 1 else "Inactive"
                output += f"- {key}: {value}\n"
            return output
        else:
            return f"Found {len(results)} records:\n" + json.dumps(results[:5], indent=2, default=str)
    
    def process_question(self, question: str) -> str:
        """Main processing function with detailed logging"""
        start_time = time.time()
        
        if not self.initialized:
            if not self.initialize():
                return "Failed to initialize system."
        
        try:
            print(f"\n{'='*60}")
            print(f"Processing Question: {question}")
            print('='*60)
            
            # Try quick patterns first
            quick_sql, pattern_info = self.quick_query_patterns(question)
            if quick_sql:
                print(f"Using {pattern_info}")
                print(f"Quick SQL: {quick_sql}")
                
                results, exec_info = self.execute_query(quick_sql)
                print(f"Execution: {exec_info}")
                
                if results is not None:
                    if len(results) == 1 and len(results[0]) == 1:
                        # Simple count/value result
                        value = list(results[0].values())[0]
                        response = f"Result: {value}"
                    else:
                        response = self.generate_response(question, results, quick_sql, exec_info)
                    
                    print(f"Response: {response}")
                    print(f"Total time: {time.time() - start_time:.2f}s")
                    return response
            
            # Fall back to LLM processing
            print("Generating SQL using LLM...")
            sql, analysis = self.generate_sql(question)
            if not sql:
                return "Could not generate valid SQL query."
            
            print(f"Question Analysis: {analysis}")
            print(f"Generated SQL: {sql}")
            
            # Execute query
            results, exec_info = self.execute_query(sql)
            print(f"Execution: {exec_info}")
            
            if results is None:
                return "Query execution failed."
            
            # Generate response
            print("Generating natural language response...")
            response = self.generate_response(question, results, sql, exec_info)
            
            print(f"Response: {response}")
            print(f"Total time: {time.time() - start_time:.2f}s")
            
            return response
            
        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            print(error_msg)
            return error_msg

def OpTestMain(query: str) -> str:
    """Main function with detailed output"""
    assistant = FastOpTestAssistant()
    try:
        return assistant.process_question(query)
    finally:
        if assistant.connection:
            assistant.connection.close()

# Test function with various query types
if __name__ == "__main__":
    test_queries = [
        "how many services are deployed",
        "tell me the layer name where server ip is 24_191",
        "how many active services are there?", 
        "show me all brokers with their status",
        "find services with thread capacity greater than 10",
        "list all servers in AADHAR_EXP layer"
    ]
    
    for query in test_queries:
        print(f"\n\nTesting Query: {query}")
        result = OpTestMain(query)
        print(f"Final Result: {result}")
        print("-" * 80)
