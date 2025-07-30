import os
import re
import logging
import pymysql
import traceback
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
import json
import calendar
import sqlglot
from sqlglot import parse_one, transpile
from sqlglot.errors import ParseError
from langchain_community.utilities import SQLDatabase
from langchain_ollama import OllamaLLM
from langchain.chains import create_sql_query_chain

today_date = date.today()

# --- OPERATIONAL TEST CONFIGURATION ---
OP_TEST_DB_CONFIG = {
    "name": "Operational Test Details",
    "db_config": {
        "host": "localhost",
        "user": "root",
        "password": "root123",
        "database": "EIS_n"  # Replace with your actual database name
    },
    "include_tables": [
        "dbOpTest_layerdetails",
        "dbOpTest_serverdetails",
        "dbOpTest_brokerdetails",
        "dbOpTest_egdetails",
        "dbOpTest_servicedetails",
        "dbOpTest_serviceadditionaldetails",
        "dbOpTest_schemas"
    ],
}

# Enhanced table schema with relationships and data analysis
TABLE_SCHEMAS = {
    "dbOpTest_layerdetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "layer_name": {"type": "varchar(100)", "not_null": True, "searchable": True}
        },
        "relationships": {
            "servers": {"table": "dbOpTest_serverdetails", "foreign_key": "layer_id", "type": "one_to_many"},
            "schemas": {"table": "dbOpTest_schemas", "via": "layer_name", "type": "one_to_many"}
        },
        "description": "Contains layer definitions for the system architecture"
    },
    
    "dbOpTest_serverdetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "serverIP": {"type": "varchar(20)", "not_null": True, "searchable": True},
            "layer_id": {"type": "bigint", "foreign_key": "dbOpTest_layerdetails.id"}
        },
        "relationships": {
            "layer": {"table": "dbOpTest_layerdetails", "foreign_key": "layer_id", "type": "many_to_one"},
            "brokers": {"table": "dbOpTest_brokerdetails", "foreign_key": "server_id", "type": "one_to_many"}
        },
        "description": "Server details including IP addresses and layer assignments"
    },
    
    "dbOpTest_brokerdetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "udpPort": {"type": "int", "not_null": True},
            "brokerName": {"type": "varchar(100)", "not_null": True, "searchable": True},
            "brokerStatus": {"type": "tinyint(1)", "not_null": True, "status_field": True},
            "server_id": {"type": "bigint", "foreign_key": "dbOpTest_serverdetails.id"}
        },
        "relationships": {
            "server": {"table": "dbOpTest_serverdetails", "foreign_key": "server_id", "type": "many_to_one"},
            "egs": {"table": "dbOpTest_egdetails", "foreign_key": "broker_id", "type": "one_to_many"},
            "schemas": {"table": "dbOpTest_schemas", "foreign_key": "brokerDetails_id", "type": "one_to_many"}
        },
        "description": "Broker configuration including ports, names, and status"
    },
    
    "dbOpTest_egdetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "egName": {"type": "varchar(100)", "not_null": True, "searchable": True},
            "egStatus": {"type": "tinyint(1)", "not_null": True, "status_field": True},
            "broker_id": {"type": "bigint", "foreign_key": "dbOpTest_brokerdetails.id"}
        },
        "relationships": {
            "broker": {"table": "dbOpTest_brokerdetails", "foreign_key": "broker_id", "type": "many_to_one"},
            "services": {"table": "dbOpTest_servicedetails", "foreign_key": "eg_id", "type": "one_to_many"},
            "schemas": {"table": "dbOpTest_schemas", "foreign_key": "egDetails_id", "type": "one_to_many"}
        },
        "description": "Execution Group details with names and status"
    },
    
    "dbOpTest_servicedetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "serviceName": {"type": "varchar(100)", "not_null": True, "searchable": True},
            "serviceStatus": {"type": "tinyint(1)", "not_null": True, "status_field": True},
            "additionalInstances": {"type": "varchar(100)", "not_null": True},
            "threadCapicity": {"type": "varchar(100)", "not_null": True},
            "threadInUse": {"type": "varchar(100)", "not_null": True},
            "timeout": {"type": "varchar(500)", "not_null": True},
            "eg_id": {"type": "bigint", "foreign_key": "dbOpTest_egdetails.id"},
            "genericName": {"type": "varchar(100)", "not_null": True, "searchable": True}
        },
        "relationships": {
            "eg": {"table": "dbOpTest_egdetails", "foreign_key": "eg_id", "type": "many_to_one"},
            "additional_details": {"table": "dbOpTest_serviceadditionaldetails", "foreign_key": "service_id", "type": "one_to_many"},
            "schemas": {"table": "dbOpTest_schemas", "foreign_key": "serviceDetails_id", "type": "one_to_many"}
        },
        "description": "Service configuration including performance metrics and status"
    },
    
    "dbOpTest_serviceadditionaldetails": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "serviceDepDate": {"type": "varchar(100)", "not_null": True, "date_field": True},
            "serviceLastEdit": {"type": "varchar(100)", "not_null": True, "date_field": True},
            "service_id": {"type": "bigint", "foreign_key": "dbOpTest_servicedetails.id"}
        },
        "relationships": {
            "service": {"table": "dbOpTest_servicedetails", "foreign_key": "service_id", "type": "many_to_one"}
        },
        "description": "Additional service details including deployment and edit dates"
    },
    
    "dbOpTest_schemas": {
        "columns": {
            "id": {"type": "bigint", "primary_key": True, "auto_increment": True},
            "layer_name": {"type": "varchar(100)", "not_null": True, "searchable": True},
            "brokerDetails_id": {"type": "bigint", "foreign_key": "dbOpTest_brokerdetails.id"},
            "egDetails_id": {"type": "bigint", "foreign_key": "dbOpTest_egdetails.id"},
            "serviceDetails_id": {"type": "bigint", "foreign_key": "dbOpTest_servicedetails.id"}
        },
        "relationships": {
            "broker": {"table": "dbOpTest_brokerdetails", "foreign_key": "brokerDetails_id", "type": "many_to_one"},
            "eg": {"table": "dbOpTest_egdetails", "foreign_key": "egDetails_id", "type": "many_to_one"},
            "service": {"table": "dbOpTest_servicedetails", "foreign_key": "serviceDetails_id", "type": "many_to_one"}
        },
        "description": "Schema mapping between layers and components"
    }
}

# Blocked patterns for security
BLOCKED_PATTERNS = [
    r"\brm\b", r"\bkill\b", r"\breboot\b", r"\bshutdown\b", r"\buserdel\b",
    r"\bpasswd\b", r"\bmkfs\b", r"\bwget\b", r"\bcurl\b", r":\s*(){:|:&};:",
    r"\bsudo\b", r"\bsu\b", r"\bchmod\b", r"\bchown\b", r"\bdd\b",
    r"\bmount\s+/", r"\bumount\b", r"\bfdisk\b", r"\bparted\b", r"\bmkfs\b",
    r"\biptables\b", r"\bufw\b", r"\bfirewall\b", r"\bselinux\b"
]

def analyze_table_relationships() -> Dict[str, Any]:
    """Analyze table relationships and create a comprehensive mapping"""
    relationships = {}
    
    for table_name, schema in TABLE_SCHEMAS.items():
        relationships[table_name] = {
            "direct_relationships": schema.get("relationships", {}),
            "searchable_fields": [col for col, info in schema["columns"].items() 
                                if info.get("searchable", False)],
            "status_fields": [col for col, info in schema["columns"].items() 
                            if info.get("status_field", False)],
            "date_fields": [col for col, info in schema["columns"].items() 
                          if info.get("date_field", False)],
            "primary_key": [col for col, info in schema["columns"].items() 
                          if info.get("primary_key", False)],
            "foreign_keys": [col for col, info in schema["columns"].items() 
                           if info.get("foreign_key")]
        }
    
    return relationships

def get_comprehensive_date_context():
    """Generate extremely comprehensive date context for robust LLM understanding"""
    current_date = date.today()
    current_year = current_date.year
    current_month = current_date.month

    # Create month name mappings (case insensitive)
    month_names = {
        'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
        'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6,
        'july': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
        'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12
    }

    month_numbers = {v: k.title() for k, v in month_names.items() if len(k) > 3}  # Full names only

    return {
        'current_date': current_date.strftime('%Y-%m-%d'),
        'current_year': current_year,
        'current_month': current_month,
        'current_month_name': month_numbers[current_month],
        'next_year': current_year + 1,
        'prev_year': current_year - 1,
        'month_names': month_names,
        'month_numbers': month_numbers,
        'years_range': list(range(current_year - 5, current_year + 10))  # Support wide range
    }

def analyze_question_intent(question: str) -> Dict[str, Any]:
    """Analyze user question to understand intent and suggest appropriate tables/joins"""
    question_lower = question.lower().strip()
    
    intent_analysis = {
        "question_type": "unknown",
        "target_tables": [],
        "search_terms": [],
        "status_query": False,
        "count_query": False,
        "date_query": False,
        "hierarchical_query": False,
        "suggested_joins": [],
        "filters": {}
    }
    
    # Detect question type
    if any(word in question_lower for word in ["how many", "count", "total"]):
        intent_analysis["question_type"] = "count"
        intent_analysis["count_query"] = True
    elif any(word in question_lower for word in ["list", "show", "display", "get", "find"]):
        intent_analysis["question_type"] = "list"
    elif any(word in question_lower for word in ["status", "active", "inactive", "enabled", "disabled"]):
        intent_analysis["status_query"] = True
    
    # Detect hierarchical queries
    hierarchy_keywords = ["layer", "server", "broker", "eg", "service"]
    mentioned_levels = [kw for kw in hierarchy_keywords if kw in question_lower]
    if len(mentioned_levels) > 1:
        intent_analysis["hierarchical_query"] = True
    
    # Map keywords to tables
    table_keywords = {
        "layer": ["dbOpTest_layerdetails"],
        "server": ["dbOpTest_serverdetails"],
        "broker": ["dbOpTest_brokerdetails"],
        "eg": ["dbOpTest_egdetails"],
        "service": ["dbOpTest_servicedetails", "dbOpTest_serviceadditionaldetails"],
        "schema": ["dbOpTest_schemas"],
        "ip": ["dbOpTest_serverdetails"],
        "port": ["dbOpTest_brokerdetails"],
        "thread": ["dbOpTest_servicedetails"],
        "deployment": ["dbOpTest_serviceadditionaldetails"],
        "date": ["dbOpTest_serviceadditionaldetails"]
    }
    
    for keyword, tables in table_keywords.items():
        if keyword in question_lower:
            intent_analysis["target_tables"].extend(tables)
    
    # Remove duplicates
    intent_analysis["target_tables"] = list(set(intent_analysis["target_tables"]))
    
    # Extract search terms (simple approach)
    # Look for quoted strings or standalone words that might be search terms
    search_patterns = [
        r'"([^"]+)"',  # Quoted strings
        r"'([^']+)'",  # Single quoted strings
        r'\b(\d+\.\d+\.\d+\.\d+)\b',  # IP addresses
        r'\b(\w+_\w+)\b'  # Underscore separated words (likely identifiers)
    ]
    
    for pattern in search_patterns:
        matches = re.findall(pattern, question_lower)
        intent_analysis["search_terms"].extend(matches)
    
    # Suggest joins based on hierarchical relationships
    if intent_analysis["hierarchical_query"]:
        intent_analysis["suggested_joins"] = suggest_joins_for_tables(intent_analysis["target_tables"])
    
    return intent_analysis

def suggest_joins_for_tables(tables: List[str]) -> List[Dict[str, str]]:
    """Suggest appropriate joins based on table relationships"""
    joins = []
    
    # Define common join patterns
    join_patterns = [
        {
            "from": "dbOpTest_layerdetails",
            "to": "dbOpTest_serverdetails",
            "condition": "dbOpTest_layerdetails.id = dbOpTest_serverdetails.layer_id"
        },
        {
            "from": "dbOpTest_serverdetails",
            "to": "dbOpTest_brokerdetails",
            "condition": "dbOpTest_serverdetails.id = dbOpTest_brokerdetails.server_id"
        },
        {
            "from": "dbOpTest_brokerdetails",
            "to": "dbOpTest_egdetails",
            "condition": "dbOpTest_brokerdetails.id = dbOpTest_egdetails.broker_id"
        },
        {
            "from": "dbOpTest_egdetails",
            "to": "dbOpTest_servicedetails",
            "condition": "dbOpTest_egdetails.id = dbOpTest_servicedetails.eg_id"
        },
        {
            "from": "dbOpTest_servicedetails",
            "to": "dbOpTest_serviceadditionaldetails",
            "condition": "dbOpTest_servicedetails.id = dbOpTest_serviceadditionaldetails.service_id"
        }
    ]
    
    # Find relevant joins for the target tables
    for pattern in join_patterns:
        if pattern["from"] in tables and pattern["to"] in tables:
            joins.append(pattern)
    
    return joins

def preprocess_question(question: str) -> str:
    """Enhanced question preprocessing with table schema awareness"""
    question_lower = question.lower().strip()
    
    # Analyze question intent
    intent = analyze_question_intent(question)
    
    # Extract explicit month-year patterns
    month_year_patterns = [
        r'\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\s+(\d{4})\b',
        r'\b(\d{4})\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\b',
        r'\b(\d{1,2})/(\d{4})\b',  # MM/YYYY
        r'\b(\d{4})/(\d{1,2})\b'   # YYYY/MM
    ]

    date_ctx = get_comprehensive_date_context()
    extracted_info = {"intent": intent}

    for pattern in month_year_patterns:
        match = re.search(pattern, question_lower)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                # Determine which is month and which is year
                for group in groups:
                    if group.isdigit():
                        num = int(group)
                        if 1900 <= num <= 2100:  # Year
                            extracted_info['year'] = num
                        elif 1 <= num <= 12:  # Month number
                            extracted_info['month'] = num
                    else:
                        # Month name
                        if group in date_ctx['month_names']:
                            extracted_info['month'] = date_ctx['month_names'][group]
            break

    # Add extracted information to question for LLM
    if extracted_info.get("intent", {}).get("target_tables"):
        addition = f" [TABLE_ANALYSIS: target_tables={extracted_info['intent']['target_tables']}, "
        addition += f"question_type={extracted_info['intent']['question_type']}, "
        addition += f"hierarchical={extracted_info['intent']['hierarchical_query']}]"
        question += addition

    if 'month' in extracted_info or 'year' in extracted_info:
        addition = f" [DATE_EXTRACTED: "
        if 'month' in extracted_info:
            addition += f"month={extracted_info['month']} "
        if 'year' in extracted_info:
            addition += f"year={extracted_info['year']} "
        addition += "]"
        question += addition

        print(f"DEBUG - Extracted info: {extracted_info}")
        print(f"DEBUG - Enhanced question: {question}")

    return question

# Setup logging
logging.basicConfig(
    filename=os.path.expanduser("~/.op_test_details_ai.log"),
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_dangerous(text: str) -> bool:
    """Check if text contains dangerous patterns"""
    return any(re.search(pattern, text.lower()) for pattern in BLOCKED_PATTERNS)

def create_table_documentation() -> str:
    """Create comprehensive table documentation for LLM context"""
    doc = "=== DATABASE SCHEMA DOCUMENTATION ===\n\n"
    
    # Add hierarchical overview
    doc += "HIERARCHICAL STRUCTURE:\n"
    doc += "Layer -> Server -> Broker -> EG -> Service -> Service Additional Details\n\n"
    
    # Add detailed table information
    for table_name, schema in TABLE_SCHEMAS.items():
        doc += f"TABLE: {table_name}\n"
        doc += f"Description: {schema['description']}\n"
        doc += "Columns:\n"
        
        for col_name, col_info in schema["columns"].items():
            doc += f"  - {col_name}: {col_info['type']}"
            if col_info.get("primary_key"):
                doc += " (PRIMARY KEY)"
            if col_info.get("foreign_key"):
                doc += f" (REFERENCES {col_info['foreign_key']})"
            if col_info.get("searchable"):
                doc += " (SEARCHABLE)"
            if col_info.get("status_field"):
                doc += " (STATUS: 1=Active, 0=Inactive)"
            doc += "\n"
        
        if schema.get("relationships"):
            doc += "Relationships:\n"
            for rel_name, rel_info in schema["relationships"].items():
                doc += f"  - {rel_name}: {rel_info['type']} with {rel_info['table']}\n"
        
        doc += "\n"
    
    return doc

def clean_markdown_from_sql(raw_sql: str) -> str:
    """BULLETPROOF: Clean markdown from SQL using only safe string operations"""
    print(f"DEBUG - Raw SQL input: {repr(raw_sql)}")

    # Start with the raw input
    clean_sql = raw_sql.strip()

    # Method 1: Handle ```sql
    if "```sql" in clean_sql:
        parts = clean_sql.split("```sql")
        if len(parts) > 1:
            sql_part = parts[1]
            if "```" in sql_part:
                clean_sql = sql_part.split("```")[0].strip()
                print(f"DEBUG - Extracted from ```sql block")

    # Method 2: Handle general ```
    elif clean_sql.count("```") >= 2:
        parts = clean_sql.split("```")
        if len(parts) >= 3:
            clean_sql = parts[1].strip()
            print(f"DEBUG - Extracted from ``` block")

    # Remove any remaining backticks
    clean_sql = clean_sql.replace("`", "")

    # Remove common prefixes
    prefixes_to_remove = [
        "here is the sql query:",
        "here's the sql query:",
        "sql query:",
        "query:",
        "here is:",
        "here's:"
    ]

    clean_lower = clean_sql.lower()
    for prefix in prefixes_to_remove:
        if clean_lower.startswith(prefix):
            clean_sql = clean_sql[len(prefix):].strip()
            break

    # Normalize whitespace
    clean_sql = ' '.join(clean_sql.split())

    print(f"DEBUG - Cleaned SQL: {clean_sql}")
    return clean_sql

def validate_sql_with_sqlglot(sql: str) -> tuple[str, bool, str]:
    """
    BULLETPROOF: Validate and parse SQL using sqlglot for enhanced security and correctness
    Returns: (cleaned_sql, is_valid, error_message)
    """
    try:
        print(f"DEBUG - SQLGlot validation input: {repr(sql)}")

        # Clean the SQL first
        clean_sql = clean_markdown_from_sql(sql)

        # Parse the SQL using sqlglot
        parsed = parse_one(clean_sql, dialect="mysql")
        print(f"DEBUG - SQLGlot parsed successfully")

        # Check if it's a SELECT statement
        if not str(parsed).upper().strip().startswith('SELECT'):
            return sql, False, "Only SELECT statements are allowed"

        # Extract all table names from the parsed query
        tables = []
        for table in parsed.find_all(sqlglot.expressions.Table):
            tables.append(table.name)

        print(f"DEBUG - Extracted tables: {tables}")

        # Validate table names against allowed tables
        allowed_tables = OP_TEST_DB_CONFIG['include_tables']
        for table in tables:
            if table not in allowed_tables:
                return sql, False, f"Table '{table}' is not in allowed tables list"

        # Check for dangerous operations in the parsed AST
        dangerous_expressions = [
            sqlglot.expressions.Insert,
            sqlglot.expressions.Update,
            sqlglot.expressions.Delete,
            sqlglot.expressions.Drop,
            sqlglot.expressions.Create,
            sqlglot.expressions.Alter,
            sqlglot.expressions.Truncate
        ]

        for dangerous_expr in dangerous_expressions:
            if parsed.find(dangerous_expr):
                return sql, False, f"Dangerous operation detected: {dangerous_expr.__name__}"

        # Transpile to clean MySQL syntax
        transpiled_result = transpile(clean_sql, read="mysql", write="mysql")
        if transpiled_result and len(transpiled_result) > 0:
            cleaned_sql = transpiled_result[0]
        else:
            cleaned_sql = clean_sql

        print(f"DEBUG - SQLGlot cleaned SQL: {cleaned_sql}")

        # Additional safety checks on the cleaned SQL
        cleaned_upper = cleaned_sql.upper()
        if not cleaned_upper.strip().startswith('SELECT'):
            return sql, False, "Transpiled SQL is not a SELECT statement"

        # Check for dangerous keywords in transpiled SQL
        dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in cleaned_upper:
                return sql, False, f"Dangerous keyword '{keyword}' found in transpiled SQL"

        return cleaned_sql, True, "Valid SQL"

    except ParseError as e:
        print(f"DEBUG - SQLGlot parse error: {e}")
        return sql, False, f"SQL parsing error: {str(e)}"
    except Exception as e:
        print(f"DEBUG - SQLGlot validation error: {e}")
        return sql, False, f"SQL validation error: {str(e)}"

def intelligent_sql_fix(raw_sql: str, question_intent: Dict[str, Any]) -> str:
    """Enhanced SQL fixing based on question intent and table analysis"""
    print(f"DEBUG - Intelligent SQL fix input: {repr(raw_sql)}")
    print(f"DEBUG - Question intent: {question_intent}")

    # Start with basic cleaning
    sql = clean_markdown_from_sql(raw_sql)

    # Handle common LLM response patterns
    if "i cannot" in sql.lower() or "i can't" in sql.lower():
        print("DEBUG - LLM refused to generate SQL")
        return "ERROR: LLM refused to generate SQL"

    # Clean up semicolons at the end
    sql = re.sub(r'\s*;?\s*$', '', sql)

    # Find SELECT statement
    select_match = re.search(r'(SELECT\s+.*?)(?:\n\s*$|$)', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        sql = select_match.group(1).strip()
        print(f"DEBUG - Extracted SELECT statement: {sql}")

    # Fix table names based on schema
    for table_name in TABLE_SCHEMAS.keys():
        short_name = table_name.replace('dbOpTest_', '')
        sql = re.sub(f'\\b{short_name}\\b', table_name, sql, flags=re.IGNORECASE)

    # Add intelligent JOIN suggestions based on intent
    if question_intent.get("hierarchical_query") and question_intent.get("suggested_joins"):
        print("DEBUG - Adding intelligent JOINs based on question intent")
        # This would require more sophisticated SQL parsing and reconstruction

    # Fix text field searches based on schema
    for table_name, schema in TABLE_SCHEMAS.items():
        for col_name, col_info in schema["columns"].items():
            if col_info.get("searchable"):
                # Convert = to LIKE for searchable text fields
                pattern = f"({col_name})\\s*=\\s*'([^']*)'"
                replacement = f"\\1 LIKE '%\\2%'"
                sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    # Fix status field queries
    status_mappings = {"active": "1", "inactive": "0", "enabled": "1", "disabled": "0"}
    for text_status, numeric_value in status_mappings.items():
        sql = re.sub(f"= '{text_status}'", f"= {numeric_value}", sql, flags=re.IGNORECASE)
        sql = re.sub(f'= "{text_status}"', f"= {numeric_value}", sql, flags=re.IGNORECASE)

    # Remove unwanted LIMIT clauses
    sql = re.sub(r'\s+LIMIT\s+\d+\s*$', '', sql, flags=re.IGNORECASE)

    # Fix spacing issues in JOIN conditions
    sql = re.sub(r'(\w+)\s*\.\s+(\w+)', r'\1.\2', sql)

    # Final cleanup - normalize whitespace
    sql = ' '.join(sql.split())

    print(f"DEBUG - Intelligently fixed SQL: {sql}")
    return sql

def validate_and_fix_sql(sql: str, question_intent: Dict[str, Any] = None) -> tuple[str, bool]:
    """Enhanced validation with intelligent fixing"""
    print(f"DEBUG - Validating SQL: {sql}")

    # First, use sqlglot for comprehensive validation
    cleaned_sql, is_valid, error_msg = validate_sql_with_sqlglot(sql)

    if not is_valid:
        print(f"DEBUG - SQLGlot validation failed: {error_msg}")

        # Try intelligent fixes based on question intent
        if question_intent:
            fixed_sql = intelligent_sql_fix(sql, question_intent)
        else:
            # Fallback to basic fixes
            fixed_sql = sql
            table_names = ['layerdetails', 'serverdetails', 'brokerdetails', 'egdetails',
                          'servicedetails', 'serviceadditionaldetails', 'schemas']

            for table in table_names:
                pattern = f'\\b{table}\\b(?!_)'
                replacement = f'dbOpTest_{table}'
                fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)

        # Try validation again with fixes
        cleaned_sql, is_valid, error_msg = validate_sql_with_sqlglot(fixed_sql)

        if not is_valid:
            print(f"DEBUG - Still invalid after fixes: {error_msg}")
            return sql, False

    # Additional safety checks
    if not cleaned_sql.upper().strip().startswith('SELECT'):
        print("DEBUG - Not a SELECT query after cleaning")
        return sql, False

    # Ensure semicolon at end
    if not cleaned_sql.strip().endswith(';'):
        cleaned_sql += ';'

    print(f"DEBUG - SQLGlot validated and cleaned SQL: {cleaned_sql}")
    return cleaned_sql, True

def analyze_sql_structure(sql: str) -> Dict[str, Any]:
    """Analyze SQL structure using sqlglot for security insights"""
    try:
        parsed = parse_one(sql, dialect="mysql")

        analysis = {
            'query_type': type(parsed).__name__,
            'tables': [],
            'columns': [],
            'functions': [],
            'has_joins': False,
            'has_subqueries': False,
            'has_aggregation': False
        }

        # Extract tables
        for table in parsed.find_all(sqlglot.expressions.Table):
            analysis['tables'].append(table.name)

        # Extract columns
        for column in parsed.find_all(sqlglot.expressions.Column):
            analysis['columns'].append(str(column))

        # Extract functions
        for func in parsed.find_all(sqlglot.expressions.Function):
            analysis['functions'].append(func.sql_name())

        # Check for joins
        analysis['has_joins'] = bool(parsed.find(sqlglot.expressions.Join))

        # Check for subqueries
        analysis['has_subqueries'] = bool(parsed.find(sqlglot.expressions.Subquery))

        # Check for aggregation
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP_CONCAT']
        analysis['has_aggregation'] = any(func.upper() in agg_functions for func in analysis['functions'])

        print(f"DEBUG - SQL Analysis: {analysis}")
        return analysis

    except Exception as e:
        print(f"DEBUG - SQL analysis error: {e}")
        return {'error': str(e)}

def sample_table_data(connection, table_name: str, limit: int = 5) -> List[Dict]:
    """Sample data from table to understand content patterns"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            return cursor.fetchall()
    except Exception as e:
        print(f"DEBUG - Error sampling {table_name}: {e}")
        return []

def analyze_data_patterns(connection) -> Dict[str, Any]:
    """Analyze actual data patterns in tables to improve query generation"""
    patterns = {}
    
    for table_name in OP_TEST_DB_CONFIG['include_tables']:
        print(f"DEBUG - Analyzing data patterns for {table_name}")
        sample_data = sample_table_data(connection, table_name, 10)
        
        if sample_data:
            patterns[table_name] = {
                'row_count': len(sample_data),
                'sample_values': {}
            }
            
            # Analyze column value patterns
            for col_name in sample_data[0].keys():
                values = [row[col_name] for row in sample_data if row[col_name] is not None]
                if values:
                    patterns[table_name]['sample_values'][col_name] = {
                        'unique_count': len(set(values)),
                        'sample_values': list(set(values))[:5],  # First 5 unique values
                        'data_type': type(values[0]).__name__ if values else 'unknown'
                    }
                    
                    # Special analysis for searchable fields
                    schema = TABLE_SCHEMAS.get(table_name, {})
                    col_info = schema.get('columns', {}).get(col_name, {})
                    if col_info.get('searchable'):
                        patterns[table_name]['sample_values'][col_name]['is_searchable'] = True
    
    print(f"DEBUG - Data patterns analysis complete: {patterns}")
    return patterns

def format_query_results_natural(result: List[Dict], question: str) -> str:
    """Enhanced result formatting with better handling of hierarchical data"""
    if not result:
        return "I couldn't find any records matching your criteria."

    # Handle single value results (like COUNT)
    if len(result) == 1 and len(result[0]) == 1:
        value = list(result[0].values())[0]
        if "count" in question.lower():
            return f"There are {value} records matching your criteria."
        else:
            return f"The result is: {value}"

    # Handle single record
    if len(result) == 1:
        record = result[0]
        response = f"I found 1 record:\n\n"

        # Format key information based on available fields
        key_fields = ['id', 'layer_name', 'serverIP', 'brokerName', 'brokerStatus',
                     'egName', 'egStatus', 'serviceName', 'serviceStatus',
                     'threadCapicity', 'threadInUse', 'genericName']

        icons = {
            'id': '🆔', 'layer_name': '🏗️', 'serverIP': '🖥️', 'brokerName': '🔗',
            'brokerStatus': '🔄', 'egName': '⚙️', 'egStatus': '🔄',
            'serviceName': '🛠️', 'serviceStatus': '🔄', 'threadCapicity': '📊',
            'threadInUse': '📈', 'genericName': '📝'
        }

        for field in key_fields:
            if field in record and record[field] is not None:
                icon = icons.get(field, '- ')
                display_name = field.replace('_', ' ').title()
                value = record[field]

                # Format status fields
                if 'status' in field.lower() and isinstance(value, int):
                    value = "Active" if value == 1 else "Inactive"

                response += f"{icon} {display_name}: {value}\n"

        return response.strip()

    # Handle multiple records
    response = f"I found {len(result)} records matching your criteria:\n\n"

    # Add status summary if available
    status_fields = ['brokerStatus', 'egStatus', 'serviceStatus']
    for status_field in status_fields:
        if status_field in result[0]:
            status_counts = {}
            for record in result:
                status = record.get(status_field, 'Unknown')
                status_display = "Active" if status == 1 else "Inactive" if status == 0 else status
                status_counts[status_display] = status_counts.get(status_display, 0) + 1

            if status_counts:
                field_name = status_field.replace('Status', '').title()
                response += f"📊 {field_name} Status Summary:\n"
                for status, count in status_counts.items():
                    response += f"   - {status}: {count} records\n"
                response += "\n"
                break  # Only show one status summary

    # Show detailed results based on count
    if len(result) <= 10:
        response += "📋 Detailed Results:\n"
        response += format_query_results_tabular(result)
    elif len(result) <= 50:
        response += f"📋 First 10 Results (of {len(result)} total):\n"
        response += format_query_results_tabular(result[:10])
        response += f"\n... and {len(result) - 10} more records."
    else:
        response += f"📋 Sample Results (showing 5 of {len(result)} total):\n"
        response += format_query_results_tabular(result[:5])
        response += f"\n... and {len(result) - 5} more records."

    return response

def format_query_results_tabular(result: List[Dict]) -> str:
    """Format results in a clean table for hierarchical data"""
    if not result:
        return "No records found."

    # Select most important columns for display based on available fields
    important_cols_priority = [
        'id', 'layer_name', 'serverIP', 'brokerName', 'brokerStatus',
        'egName', 'egStatus', 'serviceName', 'serviceStatus', 'genericName'
    ]

    available_cols = [col for col in important_cols_priority if col in result[0]]

    # If no priority columns found, use first 6 available columns
    if not available_cols:
        available_cols = list(result[0].keys())[:6]

    output = []

    # Calculate column widths
    col_widths = {}
    for col in available_cols:
        max_width = len(col)
        for row in result:
            if row.get(col) is not None:
                display_value = row[col]
                # Format status values
                if 'status' in col.lower() and isinstance(display_value, int):
                    display_value = "Active" if display_value == 1 else "Inactive"
                max_width = max(max_width, len(str(display_value)))
        col_widths[col] = min(max_width, 50)  # Max 50 chars per column

    # Header
    header = " | ".join(col.replace('_', ' ').title().ljust(col_widths[col]) for col in available_cols)
    separator = "-+-".join("-" * col_widths[col] for col in available_cols)
    output.append(header)
    output.append(separator)

    # Rows
    for row in result:
        formatted_values = []
        for col in available_cols:
            raw_value = row.get(col, 'NULL')

            # Format status fields
            if 'status' in col.lower() and isinstance(raw_value, int):
                display_value = "Active" if raw_value == 1 else "Inactive"
            else:
                display_value = str(raw_value) if raw_value is not None else 'NULL'

            # Truncate if too long
            if len(display_value) > col_widths[col]:
                display_value = display_value[:col_widths[col]-3] + "..."

            formatted_values.append(display_value.ljust(col_widths[col]))

        formatted_row = " | ".join(formatted_values)
        output.append(formatted_row)

    return "\n".join(output)

class OpTestDetailsAssistant:
    def __init__(self):
        self.llm = None
        self.db_handler = None
        self.initialized = False
        self.chat_history = []
        self.data_patterns = None
        self.table_relationships = None

    def initialize(self):
        """Initialize the Enhanced Operational Test Details Assistant"""
        try:
            # Initialize LLM with better parameters
            self.llm = OllamaLLM(model="myllm:latest", temperature=0.0)

            # Set up database connection
            db_cfg = OP_TEST_DB_CONFIG['db_config']
            uri = f"mysql+pymysql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host']}/{db_cfg['database']}"

            # Connect to database
            db_for_llm = SQLDatabase.from_uri(
                uri,
                include_tables=OP_TEST_DB_CONFIG.get("include_tables"),
                engine_args={
                    "pool_pre_ping": True,
                    "pool_recycle": 3600,
                    "connect_args": {
                        "connect_timeout": 30,
                        "read_timeout": 30,
                        "charset": "utf8mb4"
                    }
                }
            )

            # Create query chain
            chain = create_sql_query_chain(self.llm, db_for_llm)

            # Create direct connection for executing queries
            db_conn = pymysql.connect(
                host=db_cfg['host'],
                user=db_cfg['user'],
                password=db_cfg['password'],
                database=db_cfg['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                connect_timeout=30,
                read_timeout=30
            )

            self.db_handler = {
                'chain': chain,
                'connection': db_conn,
                'config': OP_TEST_DB_CONFIG
            }

            # Analyze table relationships and data patterns
            self.table_relationships = analyze_table_relationships()
            self.data_patterns = analyze_data_patterns(db_conn)

            self.initialized = True
            print("✅ Enhanced assistant initialized with data pattern analysis")
            return True

        except Exception as e:
            logger.error(f"Initialization failed: {e}\n{traceback.format_exc()}")
            return False

    def create_enhanced_context(self, question: str, question_intent: Dict[str, Any]) -> str:
        """Create enhanced context with table analysis and data patterns"""
        date_ctx = get_comprehensive_date_context()
        table_doc = create_table_documentation()
        
        context = f"""
=== ENHANCED DATABASE CONTEXT ===

CURRENT DATE CONTEXT:
- Today: {date_ctx['current_date']}
- Current Year: {date_ctx['current_year']}
- Next Year: {date_ctx['next_year']}

{table_doc}

QUESTION ANALYSIS:
- Question Type: {question_intent.get('question_type', 'unknown')}
- Target Tables: {question_intent.get('target_tables', [])}
- Hierarchical Query: {question_intent.get('hierarchical_query', False)}
- Status Query: {question_intent.get('status_query', False)}
- Count Query: {question_intent.get('count_query', False)}

DATA PATTERNS INSIGHTS:
"""
        
        # Add data pattern insights for relevant tables
        if question_intent.get('target_tables') and self.data_patterns:
            for table in question_intent['target_tables']:
                if table in self.data_patterns:
                    patterns = self.data_patterns[table]
                    context += f"\n{table} Sample Data:\n"
                    for col, col_patterns in patterns.get('sample_values', {}).items():
                        if col_patterns.get('is_searchable'):
                            context += f"  - {col}: {col_patterns['sample_values']} (searchable)\n"
                        else:
                            context += f"  - {col}: {col_patterns['sample_values']}\n"

        context += f"""

IMPORTANT SQL GENERATION RULES:
1. Use exact table names from the schema above
2. For status fields: 1 = Active/Enabled, 0 = Inactive/Disabled  
3. Use appropriate JOINs for hierarchical queries
4. Use LIKE with % wildcards for text searches
5. Handle date fields appropriately based on their string format
6. Consider the sample data patterns shown above

ORIGINAL QUESTION: {question}
"""
        
        return context

    def query_op_test_details(self, question: str) -> str:
        """Enhanced query processing with comprehensive analysis"""
        if not self.db_handler:
            return "❌ Operational Test database not available."

        try:
            print(f"DEBUG - Original question: {question}")

            # Analyze question intent
            question_intent = analyze_question_intent(question)
            print(f"DEBUG - Question intent: {question_intent}")

            # Preprocess question to extract date information
            enhanced_question = preprocess_question(question)

            # Create comprehensive context
            context_info = self.create_enhanced_context(enhanced_question, question_intent)

            # Generate SQL using the chain with enhanced context
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"DEBUG - Attempt {attempt + 1} generating SQL with enhanced context")
                    raw_sql = self.db_handler['chain'].invoke({"question": context_info})
                    print(f"DEBUG - Raw SQL from LLM: {repr(raw_sql)}")
                    break
                except Exception as e:
                    print(f"DEBUG - SQL generation failed on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return f"❌ Failed to generate SQL query after {max_retries} attempts: {str(e)}"
                    continue

            # Clean and validate SQL with intelligent fixing
            sql = intelligent_sql_fix(raw_sql, question_intent)
            sql, is_valid = validate_and_fix_sql(sql, question_intent)

            if not is_valid:
                # Try fallback approach with simpler context
                print("DEBUG - Trying fallback approach...")
                fallback_context = f"""
                Based on these tables: {', '.join(OP_TEST_DB_CONFIG['include_tables'])}
                Question: {question}
                Generate a simple SELECT query.
                """
                try:
                    raw_sql = self.db_handler['chain'].invoke({"question": fallback_context})
                    sql = intelligent_sql_fix(raw_sql, question_intent)
                    sql, is_valid = validate_and_fix_sql(sql, question_intent)
                except Exception as e:
                    return f"❌ Fallback query generation also failed: {str(e)}"

            # Analyze SQL structure for additional insights
            sql_analysis = analyze_sql_structure(sql)
            print(f"DEBUG - SQL Analysis: {sql_analysis}")

            print(f"DEBUG - Final SQL: {sql}")
            logger.info(f"Executing SQL: {sql}")

            # Execute query with error handling
            try:
                with self.db_handler['connection'].cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    print(f"DEBUG - Query returned {len(result)} rows")

                    if not result:
                        suggestions = self._generate_enhanced_suggestions(question, sql, sql_analysis, question_intent)
                        return f"I couldn't find any records matching your criteria.\n\n{suggestions}"

                    # Format and return results with enhanced formatting
                    return format_query_results_natural(result, question)

            except pymysql.Error as db_error:
                error_code = getattr(db_error, 'args', [None])[0] if hasattr(db_error, 'args') else None
                error_msg = f"❌ Database Error (Code: {error_code}): {str(db_error)}\n"
                error_msg += f"SQL: {sql}\n"

                # Provide contextual error suggestions
                error_msg += self._get_contextual_error_help(str(db_error), question_intent)
                
                logger.error(f"Database error: {db_error}\nSQL: {sql}")
                return error_msg

        except Exception as e:
            error_msg = f"❌ Error processing request: {str(e)}\n"
            error_msg += "Please try rephrasing your question."
            logger.error(f"Query processing error: {e}\n{traceback.format_exc()}")
            return error_msg

    def _get_contextual_error_help(self, error_msg: str, question_intent: Dict[str, Any]) -> str:
        """Provide contextual help based on error type and question intent"""
        help_msg = "\n💡 Contextual Help:\n"
        
        if "syntax error" in error_msg.lower():
            help_msg += "- SQL syntax error detected\n"
            if question_intent.get('hierarchical_query'):
                help_msg += "- For hierarchical queries, ensure proper JOIN syntax\n"
        elif "unknown column" in error_msg.lower():
            help_msg += "- Column name error detected\n"
            if question_intent.get('target_tables'):
                help_msg += f"- Check column names in tables: {', '.join(question_intent['target_tables'])}\n"
        elif "table" in error_msg.lower():
            help_msg += "- Table name error detected\n"
            help_msg += f"- Available tables: {', '.join(OP_TEST_DB_CONFIG['include_tables'])}\n"
        
        if question_intent.get('status_query'):
            help_msg += "- For status queries, use: 1 for Active, 0 for Inactive\n"
            
        return help_msg

    def _generate_enhanced_suggestions(self, question: str, sql: str, analysis: Dict[str, Any], question_intent: Dict[str, Any]) -> str:
        """Generate enhanced suggestions based on question intent and data patterns"""
        suggestions = "💡 Enhanced Suggestions:\n"
        
        # Basic suggestions
        suggestions += "- Try using broader search terms\n"
        suggestions += "- Check if the layer/server/service names are correct\n"
        
        # Intent-based suggestions
        if question_intent.get('status_query'):
            suggestions += "- Status values: 1 = Active/Enabled, 0 = Inactive/Disabled\n"
            
        if question_intent.get('hierarchical_query'):
            suggestions += "- Your query spans multiple hierarchy levels\n"
            if question_intent.get('suggested_joins'):
                suggestions += f"- Consider relationships: {len(question_intent['suggested_joins'])} joins suggested\n"
        
        # Data pattern suggestions
        if question_intent.get('target_tables') and self.data_patterns:
            suggestions += "\n📊 Based on actual data patterns:\n"
            for table in question_intent['target_tables']:
                if table in self.data_patterns:
                    patterns = self.data_patterns[table]
                    for col, col_patterns in patterns.get('sample_values', {}).items():
                        if col_patterns.get('is_searchable'):
                            sample_vals = col_patterns['sample_values'][:3]  # Show first 3
                            suggestions += f"- {col} examples: {', '.join(map(str, sample_vals))}\n"

        # Analysis-based suggestions
        if analysis and analysis.get('tables'):
            suggestions += f"\n🔍 Query involved tables: {', '.join(analysis['tables'])}\n"
            
        if analysis and analysis.get('has_joins'):
            suggestions += "- Query uses table joins - ensure relationships exist\n"

        suggestions += f"\n📝 Generated SQL: {sql}"
        
        return suggestions

    def process_question(self, question: str) -> str:
        """Process questions with enhanced analysis and error handling"""
        if not self.initialized and not self.initialize():
            return "❌ Enhanced Operational Test Details Assistant initialization failed. Please check database connection."

        if is_dangerous(question):
            return "❌ Question blocked for security reasons."

        # Add to chat history
        self.chat_history.append(f"User: {question}")

        # Get response with enhanced processing
        response = self.query_op_test_details(question)

        # Add response to history
        self.chat_history.append(f"Assistant: {response}")

        return response

    def start_interactive_session(self, query):
        """Process single query with comprehensive analysis and validation"""
        if not self.initialize():
            return "❌ Failed to initialize Enhanced Operational Test Details Assistant. Check database connection."

        try:
            if query.lower() in ['exit', 'quit', 'q']:
                return "👋 Session ended."

            print("🔍 Processing your query with enhanced analysis...")
            response = self.process_question(query)
            return response

        except KeyboardInterrupt:
            return "👋 Session interrupted."
        except Exception as e:
            error_msg = f"❌ Session error: {str(e)}"
            logger.error(f"Session error: {e}\n{traceback.format_exc()}")
            return error_msg
        finally:
            # Clean up database connection
            try:
                if (self.db_handler and
                    self.db_handler.get('connection') and
                    hasattr(self.db_handler['connection'], 'open') and
                    self.db_handler['connection'].open):
                    self.db_handler['connection'].close()
            except Exception as cleanup_error:
                print(f"DEBUG - Cleanup error: {cleanup_error}")
                pass

def OpTestMain(query):
    """Main function to process Operational Test queries with enhanced intelligence"""
    print("🚀 Starting Enhanced Operational Test Details Assistant...")
    assistant = OpTestDetailsAssistant()
    result = assistant.start_interactive_session(query)
    print("✅ Enhanced query processing complete.")
    return result

# Test the enhanced function
if __name__ == "__main__":
    # Test with various query types
    test_queries = [
        "tell me the layer name where server ip is 24_191",
        "how many active services are there?",
        "show me all brokers with their status",
        "find services with high thread usage",
        "list all servers in the application layer"
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Testing: {query}")
        print('='*60)
        result = OpTestMain(query)
        print(result)
