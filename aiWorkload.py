import os
import re
import logging
import pymysql
import traceback
from typing import List, Optional, Dict, Any, Tuple, Set
from datetime import datetime, date
import json
from langchain_ollama import OllamaLLM
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration with metadata"""
    name: str
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    description: str = ""
    keywords: List[str] = None

class MultiDatabaseWorkloadAI:
    def __init__(self):
        self.llm = None
        self.databases = {}
        self.connections = {}
        self.database_schemas = {}
        self.initialized = False
        
        # Initialize database configurations
        self.setup_databases()
        
    def setup_databases(self):
        """Configure all 8 databases with their metadata"""
        self.databases = {
            "EIS_n": DatabaseConfig(
                name="EIS_n",
                host="localhost",
                user="root",
                password="root123",
                database="EIS_n",
                description="Main workload management database with layers, servers, brokers, EGs, and services",
                keywords=["layer", "server", "broker", "eg", "service", "workload", "execution group"]
            ),
            "monitoring_db": DatabaseConfig(
                name="monitoring_db", 
                host="localhost",
                user="root",
                password="root123", 
                database="monitoring_db",
                description="System monitoring and metrics database",
                keywords=["monitor", "metric", "alert", "performance", "cpu", "memory", "disk"]
            ),
            "logs_db": DatabaseConfig(
                name="logs_db",
                host="localhost", 
                user="root",
                password="root123",
                database="logs_db",
                description="Application and system logs database",
                keywords=["log", "error", "debug", "trace", "audit", "event"]
            ),
            "config_db": DatabaseConfig(
                name="config_db",
                host="localhost",
                user="root", 
                password="root123",
                database="config_db",
                description="Configuration management database",
                keywords=["config", "setting", "parameter", "property", "environment"]
            ),
            "users_db": DatabaseConfig(
                name="users_db",
                host="localhost",
                user="root",
                password="root123", 
                database="users_db",
                description="User management and authentication database",
                keywords=["user", "auth", "role", "permission", "access", "login"]
            ),
            "analytics_db": DatabaseConfig(
                name="analytics_db",
                host="localhost",
                user="root",
                password="root123",
                database="analytics_db", 
                description="Analytics and reporting database",
                keywords=["analytics", "report", "dashboard", "kpi", "metric", "trend"]
            ),
            "backup_db": DatabaseConfig(
                name="backup_db",
                host="localhost",
                user="root", 
                password="root123",
                database="backup_db",
                description="Backup and recovery management database",
                keywords=["backup", "recovery", "snapshot", "restore", "archive"]
            ),
            "scheduler_db": DatabaseConfig(
                name="scheduler_db",
                host="localhost",
                user="root",
                password="root123",
                database="scheduler_db",
                description="Task scheduling and job management database", 
                keywords=["schedule", "job", "task", "cron", "batch", "queue"]
            )
        }
        
        # Define EIS_n schema (your current working database)
        self.database_schemas["EIS_n"] = {
            "dbOpTest_layerdetails": {
                "columns": ["id", "layer_name"],
                "primary_key": "id",
                "searchable": ["layer_name"],
                "description": "Layer details - AADHAR_EXP, AADHAR_SYS, ACCOUNT_EXP, ACCOUNT_SYS, CUSTOMER_EXP",
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
                "description": "Service details - serviceName like Dynatrace_Logger, panEnquiryPassThrough_expDs, thread info",
                "sample_data": "Dynatrace_Logger, panEnquiryPassThrough_expDs"
            },
            "dbOpTest_serviceadditionaldetails": {
                "columns": ["id", "serviceDepDate", "serviceLastEdit", "service_id"],
                "primary_key": "id",
                "foreign_keys": {"service_id": "dbOpTest_servicedetails.id"},
                "description": "Additional service info - deployment dates, last edit dates",
                "sample_data": "Dates like 2022-08-04_11:59:33"
            }
        }
        
    def initialize(self):
        """Initialize LLM and database connections"""
        try:
            # Initialize LLM with optimized settings for Mistral
            self.llm = OllamaLLM(
                model="mistral:7b-instruct-q4_K_M",  # Updated to use Mistral 7B
                temperature=0.1,     # Low temperature for consistent results
                timeout=60,          # Increased timeout for complex queries
                num_ctx=4096        # Context window size
            )
            
            # Test LLM connection
            test_response = self.llm.invoke("Hello, respond with 'OK' if you can understand this.")
            logger.info(f"LLM test response: {test_response}")
            
            # Initialize database connections
            self.connect_to_databases()
            
            # Discover database schemas
            self.discover_schemas()
            
            self.initialized = True
            logger.info("System initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False
    
    def connect_to_databases(self):
        """Establish connections to all databases"""
        for db_name, config in self.databases.items():
            try:
                connection = pymysql.connect(
                    host=config.host,
                    user=config.user,
                    password=config.password,
                    database=config.database,
                    port=config.port,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True,
                    connect_timeout=10,
                    read_timeout=30
                )
                self.connections[db_name] = connection
                logger.info(f"Connected to database: {db_name}")
            except Exception as e:
                logger.warning(f"Failed to connect to {db_name}: {e}")
    
    def discover_schemas(self):
        """Discover table schemas for all connected databases"""
        for db_name, connection in self.connections.items():
            if db_name in self.database_schemas:
                continue  # Skip if schema already defined
                
            try:
                schema = {}
                with connection.cursor() as cursor:
                    # Get all tables
                    cursor.execute("SHOW TABLES")
                    tables = [row[f'Tables_in_{self.databases[db_name].database}'] for row in cursor.fetchall()]
                    
                    for table in tables:
                        # Get table structure
                        cursor.execute(f"DESCRIBE {table}")
                        columns_info = cursor.fetchall()
                        
                        columns = [col['Field'] for col in columns_info]
                        primary_keys = [col['Field'] for col in columns_info if col['Key'] == 'PRI']
                        
                        schema[table] = {
                            "columns": columns,
                            "primary_key": primary_keys[0] if primary_keys else None,
                            "description": f"Table {table} in {db_name}",
                            "searchable": [col for col in columns if 'name' in col.lower() or 'desc' in col.lower()]
                        }
                
                self.database_schemas[db_name] = schema
                logger.info(f"Discovered schema for {db_name}: {len(schema)} tables")
                
            except Exception as e:
                logger.warning(f"Failed to discover schema for {db_name}: {e}")
    
    def analyze_query_intent(self, question: str) -> Dict[str, Any]:
        """Enhanced query analysis to determine which databases to query"""
        q_lower = question.lower()
        
        analysis = {
            "relevant_databases": [],
            "query_type": "unknown",
            "complexity": "simple",
            "search_terms": [],
            "cross_database": False,
            "tables_needed": {}
        }
        
        # Determine relevant databases based on keywords
        for db_name, config in self.databases.items():
            if config.keywords:
                keyword_matches = sum(1 for keyword in config.keywords if keyword in q_lower)
                if keyword_matches > 0:
                    analysis["relevant_databases"].append({
                        "database": db_name,
                        "confidence": keyword_matches / len(config.keywords),
                        "matches": [kw for kw in config.keywords if kw in q_lower]
                    })
        
        # Sort by confidence
        analysis["relevant_databases"] = sorted(
            analysis["relevant_databases"], 
            key=lambda x: x["confidence"], 
            reverse=True
        )
        
        # Determine query type
        if any(word in q_lower for word in ["count", "how many", "total", "number"]):
            analysis["query_type"] = "count"
        elif any(word in q_lower for word in ["list", "show", "display", "get", "find", "all"]):
            analysis["query_type"] = "list"
        elif any(word in q_lower for word in ["status", "active", "inactive", "health"]):
            analysis["query_type"] = "status"
        elif any(word in q_lower for word in ["when", "date", "time", "history"]):
            analysis["query_type"] = "temporal"
        elif any(word in q_lower for word in ["compare", "vs", "versus", "difference"]):
            analysis["query_type"] = "comparison"
            analysis["cross_database"] = True
        
        # Determine complexity
        if any(word in q_lower for word in ["and", "or", "both", "all", "across", "between"]):
            analysis["complexity"] = "complex"
            analysis["cross_database"] = True
        
        # Extract search terms
        ip_patterns = re.findall(r'\b\d+_\d+\b', question)
        analysis["search_terms"].extend(ip_patterns)
        
        quoted_terms = re.findall(r'["\']([^"\']+)["\']', question)
        analysis["search_terms"].extend(quoted_terms)
        
        return analysis
    
    def create_multi_database_context(self, question: str, analysis: Dict[str, Any]) -> str:
        """Create context for multi-database SQL generation"""
        relevant_dbs = [db["database"] for db in analysis["relevant_databases"][:3]]  # Top 3 most relevant
        
        if not relevant_dbs:
            relevant_dbs = ["EIS_n"]  # Default to main database
        
        context = f"""
You are an expert SQL analyst working with multiple interconnected databases in a workload management system.

QUESTION: {question}

ANALYSIS: {analysis}

AVAILABLE DATABASES AND THEIR PURPOSE:
"""
        
        for db_name in relevant_dbs:
            if db_name in self.databases:
                config = self.databases[db_name]
                context += f"\n{db_name}: {config.description}"
                context += f"\n  Keywords: {', '.join(config.keywords or [])}"
                
                if db_name in self.database_schemas:
                    schema = self.database_schemas[db_name]
                    context += f"\n  Tables: {', '.join(schema.keys())}"
                    
                    # Add detailed schema for most relevant tables
                    for table_name, table_info in list(schema.items())[:5]:
                        context += f"\n    {table_name}: {', '.join(table_info['columns'])}"
        
        context += f"""

IMPORTANT RULES:
1. Generate SQL queries for the most appropriate database(s)
2. If cross-database query is needed, create separate queries for each database
3. Use proper JOIN syntax when querying within a database
4. Status fields: 1=Active, 0=Inactive
5. Use LIKE '%term%' for name searches
6. Return JSON format if multiple queries needed:
   {{"database": "db_name", "sql": "SELECT ...", "purpose": "description"}}
7. For single database, return just the SQL query
8. Only use SELECT statements

GENERATE SQL QUERY(IES):"""
        
        return context
    
    def generate_sql_queries(self, question: str) -> List[Dict[str, str]]:
        """Generate SQL queries for one or more databases"""
        try:
            analysis = self.analyze_query_intent(question)
            context = self.create_multi_database_context(question, analysis)
            
            raw_response = self.llm.invoke(context)
            logger.info(f"LLM SQL response: {raw_response}")
            
            # Parse response - could be single SQL or JSON with multiple queries
            queries = self.parse_sql_response(raw_response, analysis)
            
            return queries
            
        except Exception as e:
            logger.error(f"SQL generation error: {e}")
            return []
    
    def parse_sql_response(self, raw_response: str, analysis: Dict[str, Any]) -> List[Dict[str, str]]:
        """Parse LLM response into structured SQL queries"""
        queries = []
        
        # Clean response
        response = raw_response.strip()
        
        # Try to parse as JSON first (multiple queries)
        try:
            if response.startswith('[') or response.startswith('{'):
                json_data = json.loads(response)
                if isinstance(json_data, list):
                    queries = json_data
                else:
                    queries = [json_data]
        except:
            # Single SQL query
            sql = self.clean_sql(response)
            if sql:
                # Determine which database to use
                db_name = "EIS_n"  # Default
                if analysis["relevant_databases"]:
                    db_name = analysis["relevant_databases"][0]["database"]
                
                queries = [{
                    "database": db_name,
                    "sql": sql,
                    "purpose": "Main query"
                }]
        
        # Validate and clean all queries
        validated_queries = []
        for query in queries:
            if "sql" in query and "database" in query:
                cleaned_sql = self.clean_sql(query["sql"])
                if cleaned_sql and query["database"] in self.connections:
                    validated_queries.append({
                        "database": query["database"],
                        "sql": cleaned_sql,
                        "purpose": query.get("purpose", "Query")
                    })
        
        return validated_queries
    
    def clean_sql(self, raw_sql: str) -> Optional[str]:
        """Clean and validate SQL query"""
        sql = raw_sql.strip()
        
        # Extract from markdown blocks
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
        prefixes = ["sql:", "query:", "here's the sql:", "select query:"]
        sql_lower = sql.lower()
        for prefix in prefixes:
            if sql_lower.startswith(prefix):
                sql = sql[len(prefix):].strip()
                break
        
        # Ensure starts with SELECT
        if not sql.upper().strip().startswith("SELECT"):
            return None
        
        # Remove trailing semicolon
        sql = sql.rstrip(";").strip()
        
        # Safety check
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"]
        if any(word in sql.upper() for word in dangerous):
            return None
        
        return sql
    
    def execute_queries(self, queries: List[Dict[str, str]]) -> Dict[str, Any]:
        """Execute multiple queries across different databases"""
        results = {
            "queries": [],
            "total_execution_time": 0,
            "success_count": 0,
            "error_count": 0
        }
        
        start_time = time.time()
        
        # Execute queries in parallel for better performance
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_query = {
                executor.submit(self.execute_single_query, query): query 
                for query in queries
            }
            
            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    result = future.result()
                    results["queries"].append(result)
                    if result["success"]:
                        results["success_count"] += 1
                    else:
                        results["error_count"] += 1
                except Exception as e:
                    results["queries"].append({
                        "database": query["database"],
                        "sql": query["sql"],
                        "purpose": query["purpose"],
                        "success": False,
                        "error": str(e),
                        "data": [],
                        "execution_time": 0
                    })
                    results["error_count"] += 1
        
        results["total_execution_time"] = time.time() - start_time
        return results
    
    def execute_single_query(self, query: Dict[str, str]) -> Dict[str, Any]:
        """Execute a single query on specified database"""
        db_name = query["database"]
        sql = query["sql"]
        
        result = {
            "database": db_name,
            "sql": sql,
            "purpose": query["purpose"],
            "success": False,
            "error": None,
            "data": [],
            "execution_time": 0
        }
        
        if db_name not in self.connections:
            result["error"] = f"Database {db_name} not connected"
            return result
        
        try:
            start_time = time.time()
            connection = self.connections[db_name]
            
            with connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
            
            result["data"] = data
            result["execution_time"] = time.time() - start_time
            result["success"] = True
            
            logger.info(f"Query executed on {db_name}: {len(data)} rows in {result['execution_time']:.3f}s")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Query failed on {db_name}: {e}")
        
        return result
    
    def generate_comprehensive_response(self, question: str, results: Dict[str, Any]) -> str:
        """Generate natural language response from multi-database results"""
        try:
            # Prepare context for LLM
            context = f"""
Generate a comprehensive answer based on multi-database query results.

ORIGINAL QUESTION: {question}

EXECUTION SUMMARY:
- Total queries executed: {len(results['queries'])}
- Successful queries: {results['success_count']}
- Failed queries: {results['error_count']}  
- Total execution time: {results['total_execution_time']:.3f}s

QUERY RESULTS:
"""
            
            for query_result in results["queries"]:
                context += f"\nDatabase: {query_result['database']}"
                context += f"\nPurpose: {query_result['purpose']}"
                context += f"\nSuccess: {query_result['success']}"
                
                if query_result["success"]:
                    data = query_result["data"][:10]  # Limit for context
                    context += f"\nRows returned: {len(query_result['data'])}"
                    context += f"\nData sample: {json.dumps(data, indent=2, default=str)}"
                else:
                    context += f"\nError: {query_result['error']}"
                context += "\n" + "-"*50
            
            context += f"""

RESPONSE GUIDELINES:
1. Provide a direct, comprehensive answer to the user's question
2. Synthesize information from all successful queries
3. Convert status numbers: 1=Active, 0=Inactive
4. If some queries failed, mention what information might be missing
5. Use natural language, avoid technical jargon
6. Be concise but complete
7. If no data found, clearly state that

NATURAL LANGUAGE RESPONSE:"""
            
            response = self.llm.invoke(context)
            return response.strip()
            
        except Exception as e:
            logger.error(f"Response generation error: {e}")
            return self.format_results_fallback(results)
    
    def format_results_fallback(self, results: Dict[str, Any]) -> str:
        """Fallback formatting when LLM response generation fails"""
        if results["success_count"] == 0:
            return "No successful queries executed. Please check your question and try again."
        
        response = f"Found information from {results['success_count']} database(s):\n\n"
        
        for query_result in results["queries"]:
            if query_result["success"]:
                data = query_result["data"]
                response += f"From {query_result['database']} ({query_result['purpose']}):\n"
                
                if len(data) == 1 and len(data[0]) == 1:
                    # Single value result
                    value = list(data[0].values())[0]
                    response += f"  Result: {value}\n"
                elif data:
                    response += f"  Found {len(data)} records\n"
                    if len(data) <= 5:
                        for row in data:
                            response += f"  - {dict(row)}\n"
                else:
                    response += "  No records found\n"
                response += "\n"
        
        return response
    
    def process_workload_question(self, question: str) -> str:
        """Main function to process workload-related questions"""
        if not self.initialized:
            if not self.initialize():
                return "System initialization failed. Please check database connections and AI model."
        
        start_time = time.time()
        
        try:
            logger.info(f"Processing question: {question}")
            
            # Generate SQL queries for relevant databases
            queries = self.generate_sql_queries(question)
            
            if not queries:
                return "Could not generate appropriate database queries for your question."
            
            logger.info(f"Generated {len(queries)} queries")
            
            # Execute queries across databases
            results = self.execute_queries(queries)
            
            # Generate comprehensive response
            response = self.generate_comprehensive_response(question, results)
            
            total_time = time.time() - start_time
            logger.info(f"Question processed in {total_time:.2f}s")
            
            return response
            
        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            logger.error(error_msg)
            return error_msg
    
    def close_connections(self):
        """Close all database connections"""
        for db_name, connection in self.connections.items():
            try:
                connection.close()
                logger.info(f"Closed connection to {db_name}")
            except:
                pass
        self.connections.clear()

def WorkloadAI(question: str) -> str:
    """Main entry point for workload AI system"""
    ai_system = MultiDatabaseWorkloadAI()
    try:
        return ai_system.process_workload_question(question)
    finally:
        ai_system.close_connections()

# Example usage and testing
if __name__ == "__main__":
    # Test various types of questions
    test_questions = [
        "How many active services are running across all systems?",
        "Show me all servers with their layer information",
        "What is the status of AADHAR_EXP brokers?",
        "List services with high thread usage",
        "Find all monitoring alerts from the last 24 hours", 
        "Compare service performance across different layers",
        "Show me backup status for all critical services",
        "What scheduled jobs are running today?",
        "Find configuration changes made this week",
        "Show user access logs for admin accounts"
    ]
    
    print("Multi-Database Workload AI System")
    print("=" * 50)
    
    for question in test_questions:
        print(f"\nQuestion: {question}")
        print("-" * 30)
        result = WorkloadAI(question)
        print(f"Answer: {result}")
        print("=" * 50)
