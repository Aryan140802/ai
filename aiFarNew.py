import os
import re
import logging
import pymysql
import traceback
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
import json
import calendar
import sqlparse
from collections import defaultdict, Counter
import pandas as pd
from langchain_community.utilities import SQLDatabase
from langchain_ollama import OllamaLLM
from langchain.chains import create_sql_query_chain

today_date = date.today()

# --- FAR DETAILS CONFIGURATION ---
FAR_DB_CONFIG = {
    "name": "FAR Details",
    "db_config": {
        "host": "localhost",
        "user": "root",
        "password": "root123",
        "database": "EIS_n"
    },
    "include_tables": ["FarDetailsAll"],
}

# Blocked patterns for security
BLOCKED_PATTERNS = [
    r"\brm\b", r"\bkill\b", r"\breboot\b", r"\bshutdown\b", r"\buserdel\b",
    r"\bpasswd\b", r"\bmkfs\b", r"\bwget\b", r"\bcurl\b", r":\s*(){:|:&};:",
    r"\bsudo\b", r"\bsu\b", r"\bchmod\b", r"\bchown\b", r"\bdd\b",
    r"\bmount\s+/", r"\bumount\b", r"\bfdisk\b", r"\bparted\b", r"\bmkfs\b",
    r"\biptables\b", r"\bufw\b", r"\bfirewall\b", r"\bselinux\b"
]

# Report templates
REPORT_TYPES = {
    "monthly": {
        "name": "Monthly FAR Report",
        "description": "Comprehensive monthly analysis of FAR requests",
        "queries": [
            "SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {month} AND YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {year}",
            "SELECT Status, COUNT(*) as count FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {month} AND YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {year} GROUP BY Status",
            "SELECT ZONE, COUNT(*) as count FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {month} AND YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = {year} GROUP BY ZONE",
            "SELECT COUNT(*) as expired_count FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = {month} AND YEAR(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = {year}"
        ]
    },
    "expiry": {
        "name": "Expiry Analysis Report",
        "description": "Analysis of FAR expiration patterns and upcoming expirations",
        "queries": [
            "SELECT * FROM FarDetailsAll WHERE STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s') BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY)",
            "SELECT ZONE, COUNT(*) as expiring_count FROM FarDetailsAll WHERE STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s') BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY) GROUP BY ZONE",
            "SELECT Status, COUNT(*) as expiring_count FROM FarDetailsAll WHERE STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s') BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY) GROUP BY Status",
            "SELECT Permanent_Rule, COUNT(*) as count FROM FarDetailsAll WHERE STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s') BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY) GROUP BY Permanent_Rule"
        ]
    },
    "security": {
        "name": "Security Analysis Report",
        "description": "Security-focused analysis of FAR requests",
        "queries": [
            "SELECT Permanent_Rule, COUNT(*) as count FROM FarDetailsAll GROUP BY Permanent_Rule",
            "SELECT ZONE, COUNT(*) as count FROM FarDetailsAll GROUP BY ZONE ORDER BY count DESC",
            "SELECT Status, COUNT(*) as count FROM FarDetailsAll GROUP BY Status",
            "SELECT * FROM FarDetailsAll WHERE Permanent_Rule = 'Yes' AND Status = 'Active'"
        ]
    },
    "trend": {
        "name": "Trend Analysis Report",
        "description": "Historical trends and patterns in FAR requests",
        "queries": [
            "SELECT YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) as year, MONTH(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) as month, COUNT(*) as count FROM FarDetailsAll GROUP BY year, month ORDER BY year DESC, month DESC LIMIT 12",
            "SELECT YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) as year, COUNT(*) as count FROM FarDetailsAll GROUP BY year ORDER BY year DESC",
            "SELECT Status, YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) as year, COUNT(*) as count FROM FarDetailsAll GROUP BY Status, year ORDER BY year DESC",
            "SELECT ZONE, YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) as year, COUNT(*) as count FROM FarDetailsAll GROUP BY ZONE, year ORDER BY year DESC"
        ]
    }
}

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

    # Check for report generation requests
    if any(term in question_lower for term in ['report', 'analysis', 'summary', 'dashboard']):
        return question  # Don't modify report requests

    # Extract explicit month-year patterns
    month_year_patterns = [
        r'\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\s+(\d{4})\b',
        r'\b(\d{4})\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\b',
        r'\b(\d{1,2})/(\d{4})\b',  # MM/YYYY
        r'\b(\d{4})/(\d{1,2})\b'   # YYYY/MM
    ]

    date_ctx = get_comprehensive_date_context()
    extracted_info = {}

    # Check for relative date terms first
    if re.search(r'\bthis\s+(month|year)\b', question_lower):
        if 'this month' in question_lower:
            extracted_info['month'] = date_ctx['current_month']
            extracted_info['year'] = date_ctx['current_year']
            extracted_info['context'] = 'this_month'
        elif 'this year' in question_lower:
            extracted_info['year'] = date_ctx['current_year']
            extracted_info['context'] = 'this_year'
    elif re.search(r'\bnext\s+(month|year)\b', question_lower):
        if 'next month' in question_lower:
            next_month = date_ctx['current_month'] + 1
            next_year = date_ctx['current_year']
            if next_month > 12:
                next_month = 1
                next_year += 1
            extracted_info['month'] = next_month
            extracted_info['year'] = next_year
            extracted_info['context'] = 'next_month'
        elif 'next year' in question_lower:
            extracted_info['year'] = date_ctx['next_year']
            extracted_info['context'] = 'next_year'
    else:
        # Extract explicit month-year patterns
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
                    extracted_info['context'] = 'specific_month_year'
                break

    # Add extracted information to question for LLM
    if extracted_info:
        addition = f" [EXTRACTED: "
        if 'month' in extracted_info:
            addition += f"month={extracted_info['month']} "
        if 'year' in extracted_info:
            addition += f"year={extracted_info['year']} "
        if 'context' in extracted_info:
            addition += f"context={extracted_info['context']} "
        addition += "]"
        question += addition

        print(f"DEBUG - Extracted date info: {extracted_info}")
        print(f"DEBUG - Enhanced question: {question}")

    return question

def get_sql_generation_prompt():
    """Generate comprehensive SQL prompt with extensive date handling examples"""
    date_ctx = get_comprehensive_date_context()

    # Generate examples for each month of current and next year
    month_examples = []
    for month_num, month_name in date_ctx['month_numbers'].items():
        current_year = date_ctx['current_year']
        next_year = date_ctx['next_year']
        month_examples.extend([
            f'"FARs created in {month_name} {current_year}" → SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Created, \'%Y-%m-%d %H:%i:%s\')) = {month_num} AND YEAR(STR_TO_DATE(Created, \'%Y-%m-%d %H:%i:%s\')) = {current_year}',
            f'"FARs expiring in {month_name.lower()} {next_year}" → SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Expires, \'%Y-%m-%d %H:%i:%s\')) = {month_num} AND YEAR(STR_TO_DATE(Expires, \'%Y-%m-%d %H:%i:%s\')) = {next_year}'
        ])

    return f"""You are an expert SQL query generator for a FAR (Firewall Access Request) database.

**CURRENT DATE CONTEXT:**
- Today's date: {date_ctx['current_date']}
- Current year: {date_ctx['current_year']}
- Current month: {date_ctx['current_month']} ({date_ctx['current_month_name']})
- Next year: {date_ctx['next_year']}
- Previous year: {date_ctx['prev_year']}

**MONTH NAME TO NUMBER MAPPING:**
{chr(10).join([f"- {name.title()}: {num}" for name, num in date_ctx['month_names'].items() if len(name) > 3])}

**DATABASE SCHEMA:**
Table: FarDetailsAll
- Far_Id (INT): Unique identifier
- Subject (TEXT): Request title
- Status (TEXT): Current status
- Created (TEXT): Creation timestamp in 'YYYY-MM-DD HH:MM:SS' format
- Expires (TEXT): Expiry timestamp in 'YYYY-MM-DD HH:MM:SS' format
- Requested_Source (TEXT): Source IPs
- Requested_Destination (TEXT): Destination IPs
- Requested_Service (TEXT): Services/ports
- Dependent_application (TEXT): Applications
- ZONE (TEXT): Environment zone
- Permanent_Rule (TEXT): Permanent status
- Other fields: Various NAT and change fields

**CRITICAL SQL GENERATION RULES:**
1. ONLY generate SELECT statements
2. Always use exact table name: FarDetailsAll
3. For date columns (Created, Expires), ALWAYS use: STR_TO_DATE(column_name, '%Y-%m-%d %H:%i:%s')
4. For text fields, use LIKE with % wildcards: column_name LIKE '%search_term%'
5. For Far_Id, use exact equality: Far_Id = number
6. NEVER add LIMIT unless specifically requested
7. Return ONLY the SQL query - no explanations or markdown
8. **CRITICAL**: When a specific month and year are mentioned (like "July 2026"), do NOT include current date comparisons
9. **CRITICAL**: For "this month" queries, ALWAYS include both month AND year filters

**DATE QUERY PATTERNS:**

**Relative Date Queries (MUST include current year):**
- "this month" → MONTH(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = {date_ctx['current_month']} AND YEAR(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = {date_ctx['current_year']}
- "this year" → YEAR(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = {date_ctx['current_year']}
- "next year" → YEAR(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = {date_ctx['next_year']}

**Specific Month-Year Queries (NO current date filters):**
- "July 2026" → MONTH(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = 7 AND YEAR(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = 2026
- "March 2025" → MONTH(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = 3 AND YEAR(STR_TO_DATE(column, '%Y-%m-%d %H:%i:%s')) = 2025

**EXTRACTED DATE INFO HANDLING:**
If question contains [EXTRACTED: month=X year=Y context=Z], use these rules:
- context=this_month → Use month AND current year, no current date filter
- context=specific_month_year → Use ONLY the specified month and year, NO current date filter
- context=this_year → Use only current year
- month=6 year=2026 context=specific_month_year → MONTH(...) = 6 AND YEAR(...) = 2026

**ROBUST QUERY EXAMPLES:**
- "FAR 123" → SELECT * FROM FarDetailsAll WHERE Far_Id = 123
- "active status" → SELECT * FROM FarDetailsAll WHERE Status LIKE '%active%'
- "this month expiring" → SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = {date_ctx['current_month']} AND YEAR(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = {date_ctx['current_year']}
- "July 2026 expiring" → SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = 7 AND YEAR(STR_TO_DATE(Expires, '%Y-%m-%d %H:%i:%s')) = 2026
- "created in march 2025" → SELECT * FROM FarDetailsAll WHERE MONTH(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = 3 AND YEAR(STR_TO_DATE(Created, '%Y-%m-%d %H:%i:%s')) = 2025

**EXAMPLES WITH EXTRACTED INFO:**
- [EXTRACTED: month=7 year=2025 context=this_month] → MONTH(...) = 7 AND YEAR(...) = 2025
- [EXTRACTED: month=6 year=2026 context=specific_month_year] → MONTH(...) = 6 AND YEAR(...) = 2026
- [EXTRACTED: year=2025 context=this_year] → YEAR(...) = 2025

**ERROR PREVENTION:**
- Always use proper STR_TO_DATE syntax
- **NEVER use CURDATE() when specific month-year is mentioned**
- Use CURDATE() ONLY for queries like "today", "from today", "current date"
- For "this month" queries, include both MONTH() = current_month AND YEAR() = current_year
- Check column names match exactly: Created, Expires (not Create, Expire)
- Use proper quotes: single quotes for strings, no quotes for numbers
- Ensure proper AND/OR logic for multiple conditions

**USER QUESTION:** {{question}}
"""

# Setup logging
logging.basicConfig(
    filename=os.path.expanduser("~/.far_details_ai.log"),
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_dangerous(text: str) -> bool:
    """Check if text contains dangerous patterns"""
    return any(re.search(pattern, text.lower()) for pattern in BLOCKED_PATTERNS)

def clean_and_fix_sql(raw_sql: str) -> str:
    """Enhanced SQL cleaning with better error handling"""
    print(f"DEBUG - Raw SQL input: {repr(raw_sql)}")
    raw_sql = sqlparse.format(raw_sql, strip_comments=True).strip()
    print(f"DEBUG - Validating SQL: {raw_sql}")
    
    # Handle common LLM response patterns
    if "i cannot" in raw_sql.lower() or "i can't" in raw_sql.lower():
        print("DEBUG - LLM refused to generate SQL")
        return "ERROR: LLM refused to generate SQL"

    # Extract from code blocks
    code_block_match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw_sql, re.DOTALL | re.IGNORECASE)
    if code_block_match:
        sql = code_block_match.group(1).strip()
        print(f"DEBUG - Extracted from code block: {sql}")
    else:
        sql = raw_sql.strip()

    # Remove common prefixes/suffixes
    sql = re.sub(r'^(here is|here\'s|sql query|query|the query is)?\s*:?\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s*;?\s*$', '', sql)

    # Find SELECT statement
    select_match = re.search(r'(SELECT\s+.*?)(?:\n\s*$|$)', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        sql = select_match.group(1).strip()
        print(f"DEBUG - Extracted SELECT statement: {sql}")

    # Handle specific FAR ID queries
    if not sql.upper().startswith('SELECT') and re.search(r'\bfar\b.*\b\d+\b', raw_sql.lower()):
        far_id_match = re.search(r'\b(\d+)\b', raw_sql)
        if far_id_match:
            far_id = far_id_match.group(1)
            sql = f"SELECT * FROM FarDetailsAll WHERE Far_Id = {far_id}"
            print(f"DEBUG - Generated FAR ID query: {sql}")

    # Fix common column name issues
    sql = re.sub(r'\bCreate\b', 'Created', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bExpire\b', 'Expires', sql, flags=re.IGNORECASE)

    # Fix text field searches (convert = to LIKE for text fields)
    text_fields = ['Subject', 'Status', 'Requested_Source', 'Requested_Destination',
                   'Requested_Service', 'Dependent_application', 'ZONE', 'Permanent_Rule']

    for field in text_fields:
        # Convert = to LIKE for text fields
        sql = re.sub(f"({field})\\s*=\\s*'([^']*)'", f"\\1 LIKE '%\\2%'", sql, flags=re.IGNORECASE)
        sql = re.sub(f"({field})\\s*=\\s*\"([^\"]*)\"", f"\\1 LIKE '%\\2%'", sql, flags=re.IGNORECASE)

    # Remove unwanted LIMIT clauses
    sql = re.sub(r'\s+LIMIT\s+\d+\s*$', '', sql, flags=re.IGNORECASE)

    # Final cleanup
    sql = sql.strip()

    print(f"DEBUG - Final cleaned SQL: {sql}")
    return sql

def validate_and_fix_sql(sql: str) -> tuple[str, bool]:
    """Validate SQL and attempt to fix common issues"""
    fixed_sql = sqlparse.format(sql, strip_comments=True).strip()
    print(f"DEBUG - Validating SQL: {fixed_sql}")
    
    # Check if it's a valid SELECT query
    if not sql.upper().strip().startswith('SELECT'):
        print("DEBUG - Not a SELECT query")
        return sql, False

    # Check for dangerous operations
    dangerous_ops = ['insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate']
    if any(op in sql.lower() for op in dangerous_ops):
        print("DEBUG - Contains dangerous operations")
        return sql, False

    # Try to fix common syntax issues
    fixed_sql = sql

    # Fix missing table name
    if 'from fardetailsall' in fixed_sql.lower():
        fixed_sql = re.sub(r'from\s+fardetailsall', 'FROM FarDetailsAll', fixed_sql, flags=re.IGNORECASE)

    # Fix date function syntax issues
    fixed_sql = re.sub(r'STR_TO_DATE\s*\(\s*([^,]+),\s*["\']%Y-%m-%d %H:%i:%s["\']\s*\)',
                      r"STR_TO_DATE(\1, '%Y-%m-%d %H:%i:%s')", fixed_sql)

    print(f"DEBUG - Fixed SQL: {fixed_sql}")
    if fixed_sql[-1] != ";":
        fixed_sql += ";"
    return fixed_sql, True

class ReportAnalyzer:
    """Comprehensive report analysis engine"""
    
    def __init__(self, data: List[Dict]):
        self.data = data
        self.df = pd.DataFrame(data) if data else pd.DataFrame()
    
    def analyze_status_distribution(self) -> Dict[str, Any]:
        """Analyze status distribution"""
        if 'Status' not in self.df.columns:
            return {"error": "Status column not found"}
        
        status_counts = self.df['Status'].value_counts().to_dict()
        total = len(self.df)
        
        analysis = {
            "total_records": total,
            "status_distribution": status_counts,
            "status_percentages": {status: round((count/total)*100, 2) 
                                 for status, count in status_counts.items()},
            "most_common_status": max(status_counts.items(), key=lambda x: x[1])[0] if status_counts else None,
            "unique_statuses": len(status_counts)
        }
        
        return analysis
    
    def analyze_zone_distribution(self) -> Dict[str, Any]:
        """Analyze zone distribution"""
        if 'ZONE' not in self.df.columns:
            return {"error": "ZONE column not found"}
        
        zone_counts = self.df['ZONE'].value_counts().to_dict()
        total = len(self.df)
        
        analysis = {
            "zone_distribution": zone_counts,
            "zone_percentages": {zone: round((count/total)*100, 2) 
                               for zone, count in zone_counts.items()},
            "most_active_zone": max(zone_counts.items(), key=lambda x: x[1])[0] if zone_counts else None,
            "unique_zones": len(zone_counts)
        }
        
        return analysis
    
    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyze temporal patterns in the data"""
        analysis = {}
        
        # Analyze creation patterns
        if 'Created' in self.df.columns:
            try:
                self.df['Created_datetime'] = pd.to_datetime(self.df['Created'])
                self.df['Created_month'] = self.df['Created_datetime'].dt.month
                self.df['Created_year'] = self.df['Created_datetime'].dt.year
                self.df['Created_day_of_week'] = self.df['Created_datetime'].dt.day_name()
                
                analysis['creation_patterns'] = {
                    "by_month": self.df['Created_month'].value_counts().sort_index().to_dict(),
                    "by_year": self.df['Created_year'].value_counts().sort_index().to_dict(),
                    "by_day_of_week": self.df['Created_day_of_week'].value_counts().to_dict(),
                    "date_range": {
                        "earliest": str(self.df['Created_datetime'].min()),
                        "latest": str(self.df['Created_datetime'].max())
                    }
                }
            except Exception as e:
                analysis['creation_patterns'] = {"error": f"Error parsing Created dates: {str(e)}"}
        
        # Analyze expiry patterns
        if 'Expires' in self.df.columns:
            try:
                self.df['Expires_datetime'] = pd.to_datetime(self.df['Expires'])
                self.df['Expires_month'] = self.df['Expires_datetime'].dt.month
                self.df['Expires_year'] = self.df['Expires_datetime'].dt.year
                
                # Check for expired vs active
                current_date = pd.Timestamp.now()
                self.df['Is_Expired'] = self.df['Expires_datetime'] < current_date
                
                analysis['expiry_patterns'] = {
                    "by_month": self.df['Expires_month'].value_counts().sort_index().to_dict(),
                    "by_year": self.df['Expires_year'].value_counts().sort_index().to_dict(),
                    "expired_count": int(self.df['Is_Expired'].sum()),
                    "active_count": int((~self.df['Is_Expired']).sum()),
                    "expiry_date_range": {
                        "earliest": str(self.df['Expires_datetime'].min()),
                        "latest": str(self.df['Expires_datetime'].max())
                    }
                }
            except Exception as e:
                analysis['expiry_patterns'] = {"error": f"Error parsing Expires dates: {str(e)}"}
        
        return analysis
    
    def analyze_security_aspects(self) -> Dict[str, Any]:
        """Analyze security-related aspects"""
        analysis = {}
        
        # Permanent rules analysis
        if 'Permanent_Rule' in self.df.columns:
            perm_counts = self.df['Permanent_Rule'].value_counts().to_dict()
            total = len(self.df)
            analysis['permanent_rules'] = {
                "distribution": perm_counts,
                "percentages": {rule: round((count/total)*100, 2) 
                              for rule, count in perm_counts.items()}
            }
        
        # Service analysis
        if 'Requested_Service' in self.df.columns:
            services = self.df['Requested_Service'].dropna()
            # Count unique services (split by common delimiters)
            all_services = []
            for service_list in services:
                if isinstance(service_list, str):
                    all_services.extend([s.strip() for s in re.split(r'[,;|\n]', service_list) if s.strip()])
            
            service_counter = Counter(all_services)
            analysis['services'] = {
                "most_common": dict(service_counter.most_common(10)),
                "unique_services": len(service_counter),
                "total_service_requests": len(all_services)
            }
        
        return analysis
    
    def generate_insights(self) -> List[str]:
        """Generate actionable insights from the data"""
        insights = []
        
        if len(self.df) == 0:
            return ["No data available for analysis."]
        
        # Status insights
        if 'Status' in self.df.columns:
            status_analysis = self.analyze_status_distribution()
            if status_analysis.get('most_common_status'):
                most_common = status_analysis['most_common_status']
                percentage = status_analysis['status_percentages'][most_common]
                insights.append(f"Most common status is '{most_common}' ({percentage}% of all records)")
            
            if status_analysis['unique_statuses'] > 5:
                insights.append(f"High status diversity detected: {status_analysis['unique_statuses']} different statuses")
        
        # Zone insights
        if 'ZONE' in self.df.columns:
            zone_analysis = self.analyze_zone_distribution()
            if zone_analysis.get('most_active_zone'):
                most_active = zone_analysis['most_active_zone']
                percentage = zone_analysis['zone_percentages'][most_active]
                insights.append(f"Most active zone is '{most_active}' ({percentage}% of all requests)")
        
        # Temporal insights
        temporal_analysis = self.analyze_temporal_patterns()
        if 'expiry_patterns' in temporal_analysis and 'expired_count' in temporal_analysis['expiry_patterns']:
            expired = temporal_analysis['expiry_patterns']['expired_count']
            active = temporal_analysis['expiry_patterns']['active_count']
            total = expired + active
            if total > 0:
                expired_pct = round((expired/total)*100, 2)
                insights.append(f"Expiry Status: {expired} expired ({expired_pct}%), {active} still active")
        
        # Security insights
        security_analysis = self.analyze_security_aspects()
        if 'permanent_rules' in security_analysis:
            perm_dist = security_analysis['permanent_rules']['distribution']
            if 'Yes' in perm_dist:
                perm_count = perm_dist['Yes']
                perm_pct = security_analysis['permanent_rules']['percentages']['Yes']
                insights.append(f"Security Alert: {perm_count} permanent rules ({perm_pct}%) - requires review")
        
        # Data quality insights
        if len(self.df) > 100:
            insights.append(f"Large dataset: {len(self.df)} records analyzed - consider segmented analysis")
        elif len(self.df) < 10:
            insights.append(f"Small dataset: {len(self.df)} records - insights may be limited")
        
        return insights

def format_query_results_natural(result: List[Dict], question: str) -> str:
    """Enhanced result formatting with better handling of large datasets"""
    if not result:
        return "I couldn't find any FAR records matching your criteria."

    # Handle single value results (like COUNT)
    if len(result) == 1 and len(result[0]) == 1:
        value = list(result[0].values())[0]
        if "count" in question.lower():
            return f"There are {value} FAR records matching your criteria."
        else:
            return f"The result is: {value}"

    # Handle single record
    if len(result) == 1:
        record = result[0]
        response = f"I found 1 FAR record:\n\n"

        # Format key information
        key_fields = ['Far_Id', 'Subject', 'Status', 'Created', 'Expires', 'ZONE',
                     'Requested_Source', 'Requested_Destination', 'Requested_Service',
                     'Dependent_application', 'Permanent_Rule']

        icons = {'Far_Id': '📋', 'Subject': '📝', 'Status': '🔄', 'Created': '📅',
                'Expires': '⏰', 'ZONE': '🌐', 'Requested_Source': '🔗',
                'Requested_Destination': '🎯', 'Requested_Service': '⚙️',
                'Dependent_application': '📱', 'Permanent_Rule': '🔒'}

        for field in key_fields:
            if field in record and record[field] is not None:
                icon = icons.get(field, '•')
                response += f"{icon} {field.replace('_', ' ')}: {record[field]}\n"

        return response.strip()

    # Handle multiple records
    response = f"I found {len(result)} FAR records matching your criteria:\n\n"

    # Add status summary if available
    if 'Status' in result[0]:
        status_counts = {}
        for record in result:
            status = record.get('Status', 'Unknown')
            status_counts[status] = status_counts.get(status, 0) + 1

        response += "📊 Status Summary:\n"
        for status, count in status_counts.items():
            response += f"   • {status}: {count} records\n"
        response += "\n"

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
    """Format results in a clean table"""
    if not result:
        return "No records found."

    # Select most important columns for display
    important_cols = ['Far_Id', 'Subject', 'Status', 'Created', 'Expires', 'ZONE']
    available_cols = [col for col in important_cols if col in result[0]]

    if not available_cols:
        available_cols = list(result[0].keys())[:6]  # First 6 columns

    output = []

    # Calculate column widths
    col_widths = {}
    for col in available_cols:
        max_width = len(col)
        for row in result:
            if row.get(col):
                max_width = max(max_width, len(str(row[col])))
        col_widths[col] = min(max_width, 50)  # Max 50 chars per column

    # Header
    header = " | ".join(col.ljust(col_widths[col]) for col in available_cols)
    separator = "-+-".join("-" * col_widths[col] for col in available_cols)
    output.append(header)
    output.append(separator)

    # Rows
    for row in result:
        formatted_row = " | ".join(
            str(row.get(col, 'NULL'))[:col_widths[col]-3] + "..."
            if len(str(row.get(col, 'NULL'))) > col_widths[col]
            else str(row.get(col, 'NULL')).ljust(col_widths[col])
            for col in available_cols
        )
        output.append(formatted_row)

    return "\n".join(output)

def format_report_output(report_data: Dict[str, Any], report_type: str) -> str:
    """Format comprehensive report output"""
    output = []
    output.append(f"📊 {REPORT_TYPES[report_type]['name']}")
    output.append(f"📋 {REPORT_TYPES[report_type]['description']}")
    output.append(f"📅 Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("="*80)
    
    # Raw data summary
    if 'raw_data' in report_data:
        raw_data = report_data['raw_data']
        output.append(f"\n📈 DATA SUMMARY")
        output.append(f"Total Records Analyzed: {sum(len(data) for data in raw_data.values())}")
        
        for query_name, data in raw_data.items():
            if data:
                output.append(f"  • {query_name}: {len(data)} records")
    
    # Analysis results
    if 'analysis' in report_data:
        analysis = report_data['analysis']
        output.append(f"\n🔍 DETAILED ANALYSIS")
        
        # Status Analysis
        if 'status_distribution' in analysis:
            status_analysis = analysis['status_distribution']
            output.append(f"\n📊 Status Distribution:")
            output.append(f"  Total Records: {status_analysis['total_records']}")
            for status, count in status_analysis['status_distribution'].items():
                percentage = status_analysis['status_percentages'][status]
                output.append(f"  • {status}: {count} ({percentage}%)")
        
        # Zone Analysis
        if 'zone_distribution' in analysis:
            zone_analysis = analysis['zone_distribution']
            output.append(f"\n🌐 Zone Distribution:")
            for zone, count in zone_analysis['zone_distribution'].items():
                percentage = zone_analysis['zone_percentages'][zone]
                output.append(f"  • {zone}: {count} ({percentage}%)")
        
        # Temporal Analysis
        if 'temporal_patterns' in analysis:
            temporal = analysis['temporal_patterns']
            output.append(f"\n📅 Temporal Analysis:")
            
            if 'creation_patterns' in temporal:
                creation = temporal['creation_patterns']
                if 'by_year' in creation:
                    output.append(f"  Creation by Year:")
                    for year, count in sorted(creation['by_year'].items()):
                        output.append(f"    • {year}: {count} requests")
                
                if 'by_month' in creation:
                    output.append(f"  Creation by Month:")
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    for month, count in sorted(creation['by_month'].items()):
                        month_name = month_names[month-1] if 1 <= month <= 12 else str(month)
                        output.append(f"    • {month_name}: {count} requests")
            
            if 'expiry_patterns' in temporal:
                expiry = temporal['expiry_patterns']
                if 'expired_count' in expiry and 'active_count' in expiry:
                    total = expiry['expired_count'] + expiry['active_count']
                    expired_pct = round((expiry['expired_count']/total)*100, 2) if total > 0 else 0
                    output.append(f"  Expiry Status:")
                    output.append(f"    • Expired: {expiry['expired_count']} ({expired_pct}%)")
                    output.append(f"    • Active: {expiry['active_count']} ({100-expired_pct}%)")
        
        # Security Analysis
        if 'security_aspects' in analysis:
            security = analysis['security_aspects']
            output.append(f"\n🔒 Security Analysis:")
            
            if 'permanent_rules' in security:
                perm = security['permanent_rules']
                output.append(f"  Permanent Rules:")
                for rule, count in perm['distribution'].items():
                    percentage = perm['percentages'][rule]
                    output.append(f"    • {rule}: {count} ({percentage}%)")
            
            if 'services' in security:
                services = security['services']
                output.append(f"  Service Analysis:")
                output.append(f"    • Unique Services: {services['unique_services']}")
                output.append(f"    • Total Service Requests: {services['total_service_requests']}")
                output.append(f"  Most Common Services:")
                for service, count in list(services['most_common'].items())[:5]:
                    output.append(f"    • {service}: {count} requests")
    
    # Insights
    if 'insights' in report_data:
        insights = report_data['insights']
        output.append(f"\n💡 KEY INSIGHTS")
        for i, insight in enumerate(insights, 1):
            output.append(f"  {i}. {insight}")
    
    # Recommendations
    if 'recommendations' in report_data:
        recommendations = report_data['recommendations']
        output.append(f"\n🎯 RECOMMENDATIONS")
        for i, rec in enumerate(recommendations, 1):
            output.append(f"  {i}. {rec}")
    
    output.append("\n" + "="*80)
    output.append("📧 For detailed data export or custom analysis, contact your system administrator.")
    
    return "\n".join(output)

class FarDetailsAssistant:
    def __init__(self):
        self.llm = None
        self.db_handler = None
        self.initialized = False
        self.chat_history = []

    def initialize(self):
        """Initialize the FAR Details Assistant"""
        try:
            # Initialize LLM with better parameters
            self.llm = OllamaLLM(model="myllm:latest", temperature=0.0)  # Lower temperature for consistency

            # Set up database connection
            db_cfg = FAR_DB_CONFIG['db_config']
            uri = f"mysql+pymysql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host']}/{db_cfg['database']}"

            # Connect to database
            db_for_llm = SQLDatabase.from_uri(
                uri,
                include_tables=FAR_DB_CONFIG.get("include_tables"),
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
                'config': FAR_DB_CONFIG
            }

            self.initialized = True
            return True

        except Exception as e:
            logger.error(f"Initialization failed: {e}\n{traceback.format_exc()}")
            return False

    def execute_query(self, sql: str) -> List[Dict]:
        """Execute a single SQL query and return results"""
        try:
            with self.db_handler['connection'].cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"ERROR executing query: {sql}\nError: {e}")
            return []

    def generate_report(self, report_type: str, **kwargs) -> Dict[str, Any]:
        """Generate comprehensive report based on type"""
        if report_type not in REPORT_TYPES:
            return {"error": f"Unknown report type: {report_type}"}
        
        report_config = REPORT_TYPES[report_type]
        date_ctx = get_comprehensive_date_context()
        
        # Execute all queries for the report
        raw_data = {}
        all_data = []
        
        for i, query_template in enumerate(report_config['queries']):
            try:
                # Format query with provided parameters
                if report_type == 'monthly':
                    month = kwargs.get('month', date_ctx['current_month'])
                    year = kwargs.get('year', date_ctx['current_year'])
                    query = query_template.format(month=month, year=year)
                else:
                    query = query_template
                
                print(f"DEBUG - Executing report query {i+1}: {query}")
                result = self.execute_query(query)
                query_name = f"query_{i+1}"
                raw_data[query_name] = result
                
                # Collect all detailed records (not aggregate queries)
                if result and len(result[0]) > 2:  # Assuming detailed records have more than 2 columns
                    all_data.extend(result)
                
            except Exception as e:
                print(f"ERROR in report query {i+1}: {e}")
                raw_data[f"query_{i+1}"] = []
        
        # Analyze the collected data
        analyzer = ReportAnalyzer(all_data)
        
        analysis = {
            'status_distribution': analyzer.analyze_status_distribution(),
            'zone_distribution': analyzer.analyze_zone_distribution(),
            'temporal_patterns': analyzer.analyze_temporal_patterns(),
            'security_aspects': analyzer.analyze_security_aspects()
        }
        
        # Generate insights
        insights = analyzer.generate_insights()
        
        # Generate recommendations based on analysis
        recommendations = self._generate_recommendations(analysis, report_type)
        
        return {
            'report_type': report_type,
            'report_name': report_config['name'],
            'description': report_config['description'],
            'generated_at': datetime.now().isoformat(),
            'parameters': kwargs,
            'raw_data': raw_data,
            'analysis': analysis,
            'insights': insights,
            'recommendations': recommendations
        }

    def _generate_recommendations(self, analysis: Dict[str, Any], report_type: str) -> List[str]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        # Status-based recommendations
        if 'status_distribution' in analysis:
            status_analysis = analysis['status_distribution']
            if 'status_distribution' in status_analysis:
                status_dist = status_analysis['status_distribution']
                
                # Check for high pending requests
                pending_statuses = ['Pending', 'Submitted', 'Under Review']
                pending_count = sum(status_dist.get(status, 0) for status in pending_statuses)
                total = status_analysis.get('total_records', 1)
                
                if pending_count / total > 0.3:  # More than 30% pending
                    recommendations.append("High pending request ratio detected - consider workflow optimization")
                
                # Check for resolved vs active ratio
                resolved_count = status_dist.get('Resolved', 0) + status_dist.get('Closed', 0)
                if resolved_count / total > 0.7:
                    recommendations.append("High resolution rate - good closure process")
                elif resolved_count / total < 0.3:
                    recommendations.append("Low resolution rate - review closure procedures")
        
        # Security-based recommendations
        if 'security_aspects' in analysis:
            security = analysis['security_aspects']
            if 'permanent_rules' in security:
                perm_rules = security['permanent_rules']
                if 'distribution' in perm_rules:
                    yes_count = perm_rules['distribution'].get('Yes', 0)
                    total_perm = sum(perm_rules['distribution'].values())
                    
                    if yes_count / total_perm > 0.5:
                        recommendations.append("High permanent rule ratio - review security policies and temporary alternatives")
                    
            if 'services' in security:
                services = security['services']
                if services.get('unique_services', 0) > 100:
                    recommendations.append("High service diversity - consider service standardization")
        
        # Temporal-based recommendations
        if 'temporal_patterns' in analysis:
            temporal = analysis['temporal_patterns']
            if 'expiry_patterns' in temporal:
                expiry = temporal['expiry_patterns']
                if 'expired_count' in expiry and 'active_count' in expiry:
                    expired = expiry['expired_count']
                    active = expiry['active_count']
                    total_exp = expired + active
                    
                    if expired / total_exp > 0.2:  # More than 20% expired
                        recommendations.append("High expired rule count - implement automated expiry notifications")
        
        # Report-type specific recommendations
        if report_type == 'monthly':
            recommendations.append("Schedule monthly reviews to track trends and patterns")
        elif report_type == 'expiry':
            recommendations.append("Implement 30-day and 7-day expiry alerts for proactive management")
        elif report_type == 'security':
            recommendations.append("Conduct quarterly security audits of permanent rules")
        elif report_type == 'trend':
            recommendations.append("Use trend data for capacity planning and resource allocation")
        
        return recommendations

    def query_far_details(self, question: str) -> str:
        """Enhanced query processing with report generation capability"""
        if not self.db_handler:
            return "❌ FAR Details database not available."

        # Check if this is a report generation request
        question_lower = question.lower()
        report_triggers = ['report', 'analysis', 'summary', 'dashboard']
        
        if any(trigger in question_lower for trigger in report_triggers):
            return self._handle_report_request(question)
        
        # Regular query processing (existing logic)
        try:
            print(f"DEBUG - Original question: {question}")

            # Preprocess question to extract date information
            enhanced_question = preprocess_question(question)

            # Get date context
            date_ctx = get_comprehensive_date_context()

            # Create comprehensive context for the LLM
            context_info = f"""
CURRENT DATE CONTEXT:
- Today: {date_ctx['current_date']}
- Current Year: {date_ctx['current_year']}
- Current Month: {date_ctx['current_month']} ({date_ctx['current_month_name']})
- Next Year: {date_ctx['next_year']}

IMPORTANT INSTRUCTIONS:
- For month names like 'June', 'March', etc., use the corresponding month number
- June = 6, March = 3, January = 1, December = 12, etc.
- Always use STR_TO_DATE function for date columns
- For 'June 2026' queries, use: MONTH(...) = 6 AND YEAR(...) = 2026
- For 'this month' queries, use: MONTH(...) = current_month AND YEAR(...) = current_year
- For 'created' or 'creation' use the Created column
- For 'expire' or 'expiry' use the Expires column
- NEVER use CURDATE() when specific month-year is mentioned
- For "this month" always include both month and year filters

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

            # Clean and validate SQL
            sql = clean_and_fix_sql(raw_sql)
            sql, is_valid = validate_and_fix_sql(sql)

            if not is_valid:
                return f"❌ Invalid SQL query generated. Raw: {repr(raw_sql)}\nCleaned: {sql}"

            print(f"DEBUG - Final SQL: {sql}")
            logger.info(f"Executing SQL: {sql}")

            # Execute query with error handling
            try:
                result = self.execute_query(sql)
                print(f"DEBUG - Query returned {len(result)} rows")

                if not result:
                    suggestions = self._generate_suggestions(question, sql)
                    return f"I couldn't find any FAR records matching your criteria.\n\n{suggestions}"

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

    def _handle_report_request(self, question: str) -> str:
        """Handle report generation requests"""
        question_lower = question.lower()
        
        # Determine report type
        report_type = None
        parameters = {}
        
        if any(word in question_lower for word in ['monthly', 'month']):
            report_type = 'monthly'
            # Extract month/year if specified
            date_ctx = get_comprehensive_date_context()
            parameters = {
                'month': date_ctx['current_month'],
                'year': date_ctx['current_year']
            }
            
            # Try to extract specific month/year
            for month_name, month_num in date_ctx['month_names'].items():
                if month_name in question_lower:
                    parameters['month'] = month_num
                    break
            
            # Extract year
            year_match = re.search(r'\b(20\d{2})\b', question)
            if year_match:
                parameters['year'] = int(year_match.group(1))
                
        elif any(word in question_lower for word in ['expiry', 'expire', 'expiration']):
            report_type = 'expiry'
        elif any(word in question_lower for word in ['security', 'permanent', 'risk']):
            report_type = 'security'
        elif any(word in question_lower for word in ['trend', 'historical', 'pattern']):
            report_type = 'trend'
        else:
            # Default to monthly report
            report_type = 'monthly'
            date_ctx = get_comprehensive_date_context()
            parameters = {
                'month': date_ctx['current_month'],
                'year': date_ctx['current_year']
            }
        
        try:
            print(f"DEBUG - Generating {report_type} report with parameters: {parameters}")
            report_data = self.generate_report(report_type, **parameters)
            
            if 'error' in report_data:
                return f"❌ Report generation failed: {report_data['error']}"
            
            # Format and return the report
            formatted_report = format_report_output(report_data, report_type)
            return formatted_report
            
        except Exception as e:
            error_msg = f"❌ Error generating report: {str(e)}"
            logger.error(f"Report generation error: {e}\n{traceback.format_exc()}")
            return error_msg

    def _generate_suggestions(self, question: str, sql: str) -> str:
        """Generate helpful suggestions when no results are found"""
        suggestions = "💡 Suggestions:\n"
        suggestions += "• Try using broader search terms\n"
        suggestions += "• Check if the date range contains data\n"
        suggestions += "• Verify status values (active, resolved, etc.)\n"

        if "june" in question.lower() or "2026" in question:
            suggestions += "• For future dates like 'June 2026', ensure data exists for that period\n"

        suggestions += f"\n🔍 Query executed: {sql}"
        return suggestions

    def process_question(self, question: str) -> str:
        """Process questions with enhanced error handling"""
        if not self.initialized and not self.initialize():
            return "❌ FAR Details Assistant initialization failed. Please check database connection."

        if is_dangerous(question):
            return "❌ Question blocked for security reasons."

        # Add to chat history
        self.chat_history.append(f"User: {question}")

        # Get response
        response = self.query_far_details(question)

        # Add response to history
        self.chat_history.append(f"Assistant: {response}")

        return response

    def start_interactive_session(self, query):
        """Process single query with comprehensive error handling"""
        if not self.initialize():
            return "❌ Failed to initialize FAR Details Assistant. Check database connection."

        try:
            if query.lower() in ['exit', 'quit', 'q']:
                return "👋 Session ended."

            print("🔍 Processing your query...")
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

def Farmain(query):
    """Main function to process FAR queries with enhanced robustness and reporting"""
    print("🚀 Starting FAR Details Assistant with Reporting & Analysis...")
    assistant = FarDetailsAssistant()
    result = assistant.start_interactive_session(query)
    print("✅ Query processing complete.")
    return result

# Test the enhanced function with report generation
if __name__ == "__main__":
    # Test with comprehensive queries including report generation
    test_queries = [
        # Regular queries
        "Show me FAR with ID 175",
        "List all FARs created in 2020",
        "Show me all FARs that expire in March 2020",
        "Count FARs created today",
        "FARs expiring this month",
        "All FARs with status resolved",
        "FARs created next year",
        "Count FARs expiring next year",
        "FARs created this year",
        "Show FARs created last year",
        "FARs expiring next month",
        "Count all active FARs created in the previous year",
        "List FARs that will expire in January 2026",
        "Show me FARs created between 2020 and 2023",
        "How many FARs are expiring in July 2026",
        
        # Report generation queries
        "Generate a monthly report for this month",
        "Create a monthly analysis report for June 2024",
        "Generate an expiry analysis report",
        "Create a security analysis report",
        "Generate a trend analysis report",
        "Show me a comprehensive monthly summary",
        "Create an expiration dashboard",
        "Generate security risk analysis",
        "Create a historical trend report",
        "Monthly report for December 2023"
    ]

    print("🧪 Testing Enhanced FAR Assistant with Reporting Capabilities")
    print("="*80)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*60}")
        print(f"Test {i:02d}: {query}")
        print('='*60)
        
        try:
            result = Farmain(query)
            print(result)
            
            # Add separator for readability
            if "📊" in result or "ANALYSIS" in result:
                print("\n" + "🎯 REPORT COMPLETED" + "\n")
                
        except Exception as e:
            print(f"❌ Error in test {i}: {str(e)}")
        
        # Add pause between tests for readability
        if i % 5 == 0:
            print(f"\n{'🔄 Batch {i//5} completed'}")
            print("="*80)

    print(f"\n🏁 All {len(test_queries)} tests completed!")
    print("="*80)

# Example usage functions for different report types
def generate_monthly_report(month=None, year=None):
    """Generate monthly report for specified month/year"""
    date_ctx = get_comprehensive_date_context()
    month = month or date_ctx['current_month']
    year = year or date_ctx['current_year']
    
    month_name = date_ctx['month_numbers'].get(month, str(month))
    query = f"Generate a monthly report for {month_name} {year}"
    
    return Farmain(query)

def generate_expiry_report():
    """Generate expiry analysis report"""
    return Farmain("Generate an expiry analysis report")

def generate_security_report():
    """Generate security analysis report"""
    return Farmain("Create a security analysis report")

def generate_trend_report():
    """Generate trend analysis report"""
    return Farmain("Generate a trend analysis report")

def generate_custom_report(report_type, **params):
    """Generate custom report with parameters"""
    if report_type == 'monthly':
        month = params.get('month')
        year = params.get('year')
        return generate_monthly_report(month, year)
    elif report_type == 'expiry':
        return generate_expiry_report()
    elif report_type == 'security':
        return generate_security_report()
    elif report_type == 'trend':
        return generate_trend_report()
    else:
        return f"❌ Unknown report type: {report_type}"

# Advanced reporting utilities
class ReportScheduler:
    """Utility class for scheduling and managing reports"""
    
    def __init__(self):
        self.scheduled_reports = []
    
    def schedule_monthly_report(self, day_of_month=1):
        """Schedule monthly report generation"""
        return {
            'type': 'monthly',
            'schedule': f"Monthly on day {day_of_month}",
            'next_run': self._calculate_next_monthly_run(day_of_month)
        }
    
    def schedule_expiry_alert(self, days_ahead=30):
        """Schedule expiry alert reports"""
        return {
            'type': 'expiry',
            'schedule': f"Every week, {days_ahead} days ahead",
            'next_run': self._calculate_next_weekly_run()
        }
    
    def _calculate_next_monthly_run(self, day_of_month):
        """Calculate next monthly report run date"""
        today = date.today()
        if today.day <= day_of_month:
            next_run = today.replace(day=day_of_month)
        else:
            # Next month
            if today.month == 12:
                next_run = date(today.year + 1, 1, day_of_month)
            else:
                next_run = date(today.year, today.month + 1, day_of_month)
        return next_run.strftime('%Y-%m-%d')
    
    def _calculate_next_weekly_run(self):
        """Calculate next weekly report run date"""
        today = date.today()
        days_until_monday = (7 - today.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        next_run = today + timedelta(days=days_until_monday)
        return next_run.strftime('%Y-%m-%d')

# Report export utilities
def export_report_to_json(report_data: Dict[str, Any], filename: str = None):
    """Export report data to JSON file"""
    if not filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"far_report_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        return f"✅ Report exported to {filename}"
    except Exception as e:
        return f"❌ Export failed: {str(e)}"

def export_report_to_csv(report_data: Dict[str, Any], filename: str = None):
    """Export report data to CSV file"""
    if not filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"far_report_{timestamp}.csv"
    
    try:
        # Extract raw data for CSV export
        all_records = []
        if 'raw_data' in report_data:
            for query_name, data in report_data['raw_data'].items():
                for record in data:
                    record['query_source'] = query_name
                    all_records.append(record)
        
        if all_records:
            df = pd.DataFrame(all_records)
            df.to_csv(filename, index=False)
            return f"✅ Report data exported to {filename}"
        else:
            return "❌ No data available for CSV export"
    except Exception as e:
        return f"❌ CSV export failed: {str(e)}"

# CLI interface for reports
def cli_report_interface():
    """Command-line interface for report generation"""
    print("🚀 FAR Reporting Assistant - CLI Interface")
    print("="*60)
    print("Available commands:")
    print("1. monthly [month] [year] - Generate monthly report")
    print("2. expiry - Generate expiry analysis")
    print("3. security - Generate security analysis")
    print("4. trend - Generate trend analysis")
    print("5. custom [query] - Execute custom query")
    print("6. exit - Exit the interface")
    print("="*60)
    
    while True:
        try:
            command = input("\n📝 Enter command: ").strip().lower()
            
            if command == 'exit':
                print("👋 Goodbye!")
                break
            elif command.startswith('monthly'):
                parts = command.split()
                month = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                year = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                result = generate_monthly_report(month, year)
                print(result)
            elif command == 'expiry':
                result = generate_expiry_report()
                print(result)
            elif command == 'security':
                result = generate_security_report()
                print(result)
            elif command == 'trend':
                result = generate_trend_report()
                print(result)
            elif command.startswith('custom'):
                query = input("Enter your custom query: ")
                result = Farmain(query)
                print(result)
            else:
                print("❌ Unknown command. Type 'exit' to quit.")
                
        except KeyboardInterrupt:
            print("\n👋 Session interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {str(e)}")

# Documentation and help
def print_usage_guide():
    """Print comprehensive usage guide"""
    guide = """
🚀 FAR Details Assistant with Reporting & Analysis - Usage Guide
===============================================================

BASIC QUERIES:
- "Show me FAR with ID 123"
- "List all FARs created in 2024"
- "FARs expiring this month"
- "Count active FARs"
- "Show FARs in DMZ zone"

REPORT GENERATION:
- "Generate a monthly report"
- "Create monthly analysis for June 2024"
- "Generate expiry analysis report"
- "Create security analysis report"
- "Generate trend analysis report"

DATE FORMATS SUPPORTED:
- "this month", "next month", "this year"
- "June 2024", "March 2025"
- "2024", "2023-2025"
- Specific dates: "2024-06-15"

REPORT TYPES:
1. Monthly Report: Comprehensive monthly analysis
2. Expiry Report: Upcoming expirations and patterns
3. Security Report: Security-focused analysis
4. Trend Report: Historical trends and patterns

ADVANCED FEATURES:
- Automatic date recognition and processing
- Intelligent SQL generation
- Comprehensive data analysis
- Actionable insights and recommendations
- Multiple output formats (text, JSON, CSV)

EXAMPLES:
- Farmain("Generate monthly report for June 2024")
- Farmain("Show expiry analysis")
- Farmain("Create security dashboard")
- Farmain("List FARs expiring in July 2026")

For more help, contact your system administrator.
===============================================================
"""
    print(guide)

if __name__ == "__main__":
    # If running directly, show usage guide
    print_usage_guide()
