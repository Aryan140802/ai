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
from collections import defaultdict

today_date = date.today()

# --- OPERATIONAL TEST CONFIGURATION ---
OP_TEST_DB_CONFIG = {
    "name": "Operational Test Details",
    "db_config": {
        "host": "localhost",
        "user": "root",
        "password": "root123",
        "database": "EIS_n"
    },
    "include_tables": [
        "dbOpTest_layerdetails",
        "dbOpTest_serverdetails",
        "dbOpTest_brokerdetails",
        "dbOpTest_egdetails",
        "dbOpTest_servicedetails",
        "dbOpTest_serviceadditionaldetails"
    ],
    "schema_hierarchy": {
        "dbOpTest_layerdetails": {
            "primary_key": "id",
            "children": {
                "dbOpTest_serverdetails": {
                    "foreign_key": "layer_id",
                    "relationship": "one-to-many"
                }
            }
        },
        "dbOpTest_serverdetails": {
            "primary_key": "id",
            "parent": "dbOpTest_layerdetails",
            "children": {
                "dbOpTest_brokerdetails": {
                    "foreign_key": "server_id",
                    "relationship": "one-to-many"
                }
            }
        },
        "dbOpTest_brokerdetails": {
            "primary_key": "id",
            "parent": "dbOpTest_serverdetails",
            "children": {
                "dbOpTest_egdetails": {
                    "foreign_key": "broker_id",
                    "relationship": "one-to-many"
                }
            }
        },
        "dbOpTest_egdetails": {
            "primary_key": "id",
            "parent": "dbOpTest_brokerdetails",
            "children": {
                "dbOpTest_servicedetails": {
                    "foreign_key": "eg_id",
                    "relationship": "one-to-many"
                }
            }
        },
        "dbOpTest_servicedetails": {
            "primary_key": "id",
            "parent": "dbOpTest_egdetails",
            "children": {
                "dbOpTest_serviceadditionaldetails": {
                    "foreign_key": "service_id",
                    "relationship": "one-to-one"
                }
            }
        },
        "dbOpTest_serviceadditionaldetails": {
            "primary_key": "id",
            "parent": "dbOpTest_servicedetails"
        }
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

def preprocess_question(question: str) -> str:
    """Preprocess the question to handle common patterns and extract explicit date info"""
    question_lower = question.lower().strip()

    # Extract explicit month-year patterns
    month_year_patterns = [
        r'\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\s+(\d{4})\b',
        r'\b(\d{4})\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\b',
        r'\b(\d{1,2})/(\d{4})\b',  # MM/YYYY
        r'\b(\d{4})/(\d{1,2})\b'   # YYYY/MM
    ]

    date_ctx = get_comprehensive_date_context()
    extracted_info = {}

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
    if extracted_info:
        addition = f" [EXTRACTED: "
        if 'month' in extracted_info:
            addition += f"month={extracted_info['month']} "
        if 'year' in extracted_info:
            addition += f"year={extracted_info['year']} "
        addition += "]"
        question += addition

        print(f"DEBUG - Extracted date info: {extracted_info}")
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

def clean_markdown_from_sql(raw_sql: str) -> str:
    """BULLETPROOF: Clean markdown from SQL using only safe string operations"""
    print(f"DEBUG - Raw SQL input: {repr(raw_sql)}")

    # Start with the raw input
    clean_sql = raw_sql.strip()

    # Method 1: Handle ```
    if "```sql" in clean_sql:
        parts = clean_sql.split("```")
        if len(parts) > 1:
            sql_part = parts[1]
            if "```" in sql_part:
                clean_sql = sql_part.split("```")[0]
                print(f"DEBUG - Extracted from ```sql block")

    # Method 2: Handle ```
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

def clean_and_fix_sql(raw_sql: str) -> str:
    """BULLETPROOF: Enhanced SQL cleaning with safe string operations"""
    print(f"DEBUG - Raw SQL input: {repr(raw_sql)}")

    # Handle common LLM response patterns
    if "i cannot" in raw_sql.lower() or "i can't" in raw_sql.lower():
        print("DEBUG - LLM refused to generate SQL")
        return "ERROR: LLM refused to generate SQL"

    # Clean markdown
    sql = clean_markdown_from_sql(raw_sql)

    # Clean up semicolons at the end
    sql = re.sub(r'\s*;?\s*$', '', sql)

    # Find SELECT statement
    select_match = re.search(r'(SELECT\s+.*?)(?:\n\s*$|$)', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        sql = select_match.group(1).strip()
        print(f"DEBUG - Extracted SELECT statement: {sql}")

    # Fix common table name issues
    table_mappings = {
        'layerdetails': 'dbOpTest_layerdetails',
        'serverdetails': 'dbOpTest_serverdetails',
        'brokerdetails': 'dbOpTest_brokerdetails',
        'egdetails': 'dbOpTest_egdetails',
        'servicedetails': 'dbOpTest_servicedetails',
        'serviceadditionaldetails': 'dbOpTest_serviceadditionaldetails'
    }

    for old_name, new_name in table_mappings.items():
        sql = re.sub(f'\\b{old_name}\\b', new_name, sql, flags=re.IGNORECASE)

    # Fix text field searches (convert = to LIKE for varchar fields)
    text_fields = ['layer_name', 'serverIP', 'brokerName', 'egName', 'serviceName',
                   'additionalInstances', 'threadCapicity', 'threadInUse', 'timeout',
                   'genericName', 'serviceDepDate', 'serviceLastEdit']

    for field in text_fields:
        # Convert = to LIKE for text fields
        sql = re.sub(f"({field})\\s*=\\s*'([^']*)'", f"\\1 LIKE '%\\2%'", sql, flags=re.IGNORECASE)
        sql = re.sub(f"({field})\\s*=\\s*\"([^\"]*)\"", f"\\1 LIKE '%\\2%'", sql, flags=re.IGNORECASE)

    # Remove unwanted LIMIT clauses
    sql = re.sub(r'\s+LIMIT\s+\d+\s*$', '', sql, flags=re.IGNORECASE)

    # Fix spacing issues in JOIN conditions (remove extra spaces around dots)
    sql = re.sub(r'(\w+)\s*\.\s+(\w+)', r'\1.\2', sql)

    # Final cleanup - normalize whitespace
    sql = ' '.join(sql.split())

    print(f"DEBUG - Final cleaned SQL: {sql}")
    return sql

def validate_and_fix_sql(sql: str) -> tuple[str, bool]:
    """Validate SQL using sqlglot and attempt to fix common issues"""
    print(f"DEBUG - Validating SQL: {sql}")

    # First, use sqlglot for comprehensive validation
    cleaned_sql, is_valid, error_msg = validate_sql_with_sqlglot(sql)

    if not is_valid:
        print(f"DEBUG - SQLGlot validation failed: {error_msg}")

        # Try some basic fixes and re-validate
        fixed_sql = sql

        # Fix missing table name prefixes
        table_names = ['layerdetails', 'serverdetails', 'brokerdetails', 'egdetails',
                       'servicedetails', 'serviceadditionaldetails']

        for table in table_names:
            # Add dbOpTest_ prefix if missing
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

def analyze_performance_metrics(results: List[Dict]) -> Dict[str, Any]:
    """Analyze performance metrics from service details"""
    if not results:
        return {}
    
    analysis = {
        'total_services': len(results),
        'active_services': 0,
        'inactive_services': 0,
        'total_thread_capacity': 0,
        'total_threads_in_use': 0,
        'utilization_stats': {
            'high': 0,
            'medium': 0,
            'low': 0
        },
        'service_status_distribution': defaultdict(int),
        'eg_distribution': defaultdict(int),
        'broker_distribution': defaultdict(int)
    }
    
    for record in results:
        # Count active/inactive services
        if 'serviceStatus' in record:
            status = record['serviceStatus']
            if status == 1:
                analysis['active_services'] += 1
            else:
                analysis['inactive_services'] += 1
        
        # Analyze thread utilization if available
        if 'threadCapicity' in record and 'threadInUse' in record:
            try:
                capacity = int(record['threadCapicity'])
                in_use = int(record['threadInUse'])
                analysis['total_thread_capacity'] += capacity
                analysis['total_threads_in_use'] += in_use
                
                # Calculate utilization percentage
                if capacity > 0:
                    utilization = (in_use / capacity) * 100
                    if utilization > 75:
                        analysis['utilization_stats']['high'] += 1
                    elif utilization > 25:
                        analysis['utilization_stats']['medium'] += 1
                    else:
                        analysis['utilization_stats']['low'] += 1
            except (ValueError, TypeError):
                pass
        
        # Track distributions
        if 'serviceName' in record:
            analysis['service_status_distribution'][record['serviceName']] += 1
        if 'egName' in record:
            analysis['eg_distribution'][record['egName']] += 1
        if 'brokerName' in record:
            analysis['broker_distribution'][record['brokerName']] += 1
    
    # Calculate overall utilization percentage
    if analysis['total_thread_capacity'] > 0:
        analysis['overall_utilization'] = (analysis['total_threads_in_use'] / analysis['total_thread_capacity']) * 100
    else:
        analysis['overall_utilization'] = 0
    
    return analysis

def predict_service_trends(results: List[Dict]) -> Dict[str, Any]:
    """Generate predictions based on service data"""
    if not results:
        return {}
    
    # Simple prediction logic based on current utilization
    prediction = {
        'capacity_needed': False,
        'potential_bottlenecks': [],
        'underutilized_services': [],
        'recommendations': []
    }
    
    for record in results:
        if 'threadCapicity' in record and 'threadInUse' in record and 'serviceName' in record:
            try:
                capacity = int(record['threadCapicity'])
                in_use = int(record['threadInUse'])
                
                if capacity > 0:
                    utilization = (in_use / capacity) * 100
                    
                    # Identify potential bottlenecks
                    if utilization > 90:
                        prediction['potential_bottlenecks'].append({
                            'service': record['serviceName'],
                            'utilization': f"{utilization:.1f}%",
                            'capacity': capacity,
                            'in_use': in_use
                        })
                    
                    # Identify underutilized services
                    elif utilization < 10:
                        prediction['underutilized_services'].append({
                            'service': record['serviceName'],
                            'utilization': f"{utilization:.1f}%",
                            'capacity': capacity,
                            'in_use': in_use
                        })
            except (ValueError, TypeError):
                continue
    
    # Generate recommendations
    if prediction['potential_bottlenecks']:
        prediction['capacity_needed'] = True
        prediction['recommendations'].append(
            "Consider increasing thread capacity for high-utilization services"
        )
    
    if prediction['underutilized_services']:
        prediction['recommendations'].append(
            "Consider consolidating or reducing capacity for underutilized services"
        )
    
    return prediction

def format_query_results_natural(result: List[Dict], question: str) -> str:
    """Enhanced result formatting with better handling of hierarchical data"""
    if not result:
        # Perform analysis even when no results to provide better feedback
        suggestions = "No records found matching your criteria."
        
        # Check if this might be a hierarchical query
        if any(keyword in question.lower() for keyword in ['layer', 'server', 'broker', 'eg', 'service']):
            suggestions += "\n\n💡 Try navigating the hierarchy:\n"
            suggestions += "- Layers → Servers → Brokers → EGs → Services\n"
            suggestions += "- Example: 'Show services under broker AADHAR_EXP'"
        
        return suggestions

    # Handle single value results (like COUNT)
    if len(result) == 1 and len(result[0]) == 1:
        value = list(result[0].values())[0]
        if "count" in question.lower():
            return f"There are {value} records matching your criteria."
        else:
            return f"The result is: {value}"

    # Perform performance analysis
    performance = analyze_performance_metrics(result)
    predictions = predict_service_trends(result)

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

        # Add performance insights if available
        if 'threadCapicity' in record and 'threadInUse' in record:
            try:
                capacity = int(record['threadCapicity'])
                in_use = int(record['threadInUse'])
                if capacity > 0:
                    utilization = (in_use / capacity) * 100
                    response += f"\n📈 Utilization: {utilization:.1f}% ({in_use} of {capacity} threads)"
            except (ValueError, TypeError):
                pass

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

    # Add performance analysis
    if performance:
        response += "\n\n📊 Performance Analysis:\n"
        response += f"- Active Services: {performance['active_services']} ({performance['active_services']/len(result)*100:.1f}%)\n"
        response += f"- Thread Utilization: {performance['overall_utilization']:.1f}%\n"
        response += f"  - High (>75%): {performance['utilization_stats']['high']} services\n"
        response += f"  - Medium (25-75%): {performance['utilization_stats']['medium']} services\n"
        response += f"  - Low (<25%): {performance['utilization_stats']['low']} services\n"

    # Add predictions if available
    if predictions:
        if predictions['potential_bottlenecks']:
            response += "\n⚠️ Potential Bottlenecks:\n"
            for bottleneck in predictions['potential_bottlenecks'][:3]:  # Show top 3
                response += f"- {bottleneck['service']} at {bottleneck['utilization']} utilization\n"
        
        if predictions['underutilized_services']:
            response += "\nℹ️ Underutilized Services:\n"
            for service in predictions['underutilized_services'][:3]:  # Show top 3
                response += f"- {service['service']} at {service['utilization']} utilization\n"
        
        if predictions['recommendations']:
            response += "\n💡 Recommendations:\n"
            for rec in predictions['recommendations']:
                response += f"- {rec}\n"

    return response

def format_query_results_tabular(result: List[Dict]) -> str:
    """Format results in a clean table for hierarchical data"""
    if not result:
        return "No records found."

    # Select most important columns for display based on available fields
    important_cols_priority = [
        'id', 'layer_name', 'serverIP', 'brokerName', 'brokerStatus',
        'egName', 'egStatus', 'serviceName', 'serviceStatus', 'genericName',
        'threadCapicity', 'threadInUse'
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

    def initialize(self):
        """Initialize the Operational Test Details Assistant"""
        try:
            # Initialize LLM with better parameters
            self.llm = OllamaLLM(model="myllm:latest", temperature=0.0)  # Lower temperature for consistency

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

            self.initialized = True
            return True

        except Exception as e:
            logger.error(f"Initialization failed: {e}\n{traceback.format_exc()}")
            return False

    def query_op_test_details(self, question: str) -> str:
        """Enhanced query processing with sqlglot integration"""
        if not self.db_handler:
            return "❌ Operational Test database not available."

        try:
            print(f"DEBUG - Original question: {question}")

            # Preprocess question to extract date information
            enhanced_question = preprocess_question(question)

            # Get date context
            date_ctx = get_comprehensive_date_context()

            # Create comprehensive context for the LLM
            context_info = f"""
DATABASE SCHEMA HIERARCHY:
Layer (dbOpTest_layerdetails) → Server (dbOpTest_serverdetails) → Broker (dbOpTest_brokerdetails) → EG (dbOpTest_egdetails) → Service (dbOpTest_servicedetails) → Service Additional Details (dbOpTest_serviceadditionaldetails)

KEY RELATIONSHIPS:
- layer_id in serverdetails links to layerdetails.id
- server_id in brokerdetails links to serverdetails.id
- broker_id in egdetails links to brokerdetails.id
- eg_id in servicedetails links to egdetails.id
- service_id in serviceadditionaldetails links to servicedetails.id

STATUS FIELDS:
- brokerStatus: 1 = Active, 0 = Inactive
- egStatus: 1 = Active, 0 = Inactive
- serviceStatus: 1 = Active, 0 = Inactive

PERFORMANCE METRICS:
- threadCapicity: Total available threads
- threadInUse: Currently used threads
- timeout: Service timeout configuration

CURRENT DATE CONTEXT:
- Today: {date_ctx['current_date']}
- Current Year: {date_ctx['current_year']}
- Next Year: {date_ctx['next_year']}

IMPORTANT INSTRUCTIONS:
- Use exact table names with dbOpTest_ prefix
- For text searches, use LIKE with % wildcards
- When querying across hierarchy, use proper JOINs
- For status checks, use = 1 for active, = 0 for inactive
- For performance analysis, consider threadCapicity vs threadInUse

QUESTION: {enhanced_question}
"""

            # Generate SQL using the chain
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"DEBUG - Attempt {attempt + 1} generating SQL")
                    raw_sql = self.db_handler['chain'].invoke({"question": context_info})
                    print(f"DEBUG - Raw SQL from LLM: {repr(raw_sql)}")
                    break
                except Exception as e:
                    print(f"DEBUG - SQL generation failed on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return f"❌ Failed to generate SQL query after {max_retries} attempts: {str(e)}"
                    continue

            # Clean and validate SQL with sqlglot
            sql = clean_and_fix_sql(raw_sql)
            sql, is_valid = validate_and_fix_sql(sql)

            if not is_valid:
                return f"❌ Invalid SQL query generated. Raw: {repr(raw_sql)}\nCleaned: {sql}"

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
                        suggestions = self._generate_suggestions(question, sql, sql_analysis)
                        return f"I couldn't find any records matching your criteria.\n\n{suggestions}"

                    # Format and return results
                    return format_query_results_natural(result, question)

            except pymysql.Error as db_error:
                error_code = getattr(db_error, 'args', [None])[0] if hasattr(db_error, 'args') else None
                error_msg = f"❌ Database Error (Code: {error_code}): {str(db_error)}\n"
                error_msg += f"SQL: {sql}\n"

                # Try to provide helpful suggestions based on error type
                if "syntax error" in str(db_error).lower():
                    error_msg += "This appears to be a SQL syntax error. "
                elif "unknown column" in str(db_error).lower():
                    error_msg += "This appears to be a column name error. "
                elif "table" in str(db_error).lower():
                    error_msg += "This appears to be a table name error. "

                error_msg += "Please try rephrasing your question."
                logger.error(f"Database error: {db_error}\nSQL: {sql}")
                return error_msg

        except Exception as e:
            error_msg = f"❌ Error processing request: {str(e)}\n"
            error_msg += "Please try rephrasing your question."
            logger.error(f"Query processing error: {e}\n{traceback.format_exc()}")
            return error_msg

    def _generate_suggestions(self, question: str, sql: str, analysis: Dict[str, Any]) -> str:
        """Generate helpful suggestions when no results are found"""
        suggestions = "💡 Suggestions:\n"
        suggestions += "- Try using broader search terms\n"
        suggestions += "- Check if the layer/server/service names are correct\n"
        suggestions += "- Verify status values (use 1 for active, 0 for inactive)\n"
        suggestions += "- Consider checking different levels of the hierarchy\n"

        if "status" in question.lower():
            suggestions += "- Status values: 1 = Active/Enabled, 0 = Inactive/Disabled\n"

        if analysis and analysis.get('has_joins'):
            suggestions += "- Your query involves multiple tables - ensure relationships exist\n"

        if analysis and analysis.get('tables'):
            suggestions += f"- Tables involved: {', '.join(analysis['tables'])}\n"

        # Add hierarchical navigation tips
        if any(keyword in question.lower() for keyword in ['layer', 'server', 'broker', 'eg', 'service']):
            suggestions += "\n🔍 Navigate the hierarchy:\n"
            suggestions += "1. Layers contain Servers\n"
            suggestions += "2. Servers contain Brokers\n"
            suggestions += "3. Brokers contain EGs\n"
            suggestions += "4. EGs contain Services\n"
            suggestions += "Example queries:\n"
            suggestions += "- 'Show servers in layer AADHAR_EXP'\n"
            suggestions += "- 'Show services under broker AADHAR_EXP'\n"
            suggestions += "- 'Find inactive services in layer AADHAR_EXP'"

        suggestions += f"\n🔍 Query executed: {sql}"
        if analysis:
            suggestions += f"\n📊 Query analysis: {analysis.get('query_type', 'Unknown')} with {len(analysis.get('tables', []))} tables"

        return suggestions

    def process_question(self, question: str) -> str:
        """Process questions with enhanced error handling and sqlglot integration"""
        if not self.initialized and not self.initialize():
            return "❌ Operational Test Details Assistant initialization failed. Please check database connection."

        if is_dangerous(question):
            return "❌ Question blocked for security reasons."

        # Add to chat history
        self.chat_history.append(f"User: {question}")

        # Get response
        response = self.query_op_test_details(question)

        # Add response to history
        self.chat_history.append(f"Assistant: {response}")

        return response

    def start_interactive_session(self, query):
        """Process single query with comprehensive error handling and sqlglot validation"""
        if not self.initialize():
            return "❌ Failed to initialize Operational Test Details Assistant. Check database connection."

        try:
            if query.lower() in ['exit', 'quit', 'q']:
                return "👋 Session ended."

            print("🔍 Processing your query with SQLGlot validation...")
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
    """Main function to process Operational Test queries with sqlglot integration"""
    print("🚀 Starting Operational Test Details Assistant with SQLGlot...")
    assistant = OpTestDetailsAssistant()
    result = assistant.start_interactive_session(query)
    print("✅ Query processing complete.")
    return result

# Test the function with hierarchical queries and sqlglot validation
if __name__ == "__main__":
    # Test with comprehensive hierarchical queries
    test_queries = [
        "tell me the layer name where server ip is 24_191",
        "show active services under broker AADHAR_EXP",
        "what is the thread utilization for services in layer AADHAR_EXP?",
        "find inactive brokers",
        "show services with high thread utilization (>90%)"
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Testing: {query}")
        print('='*60)
        result = OpTestMain(query)
        print(result)
