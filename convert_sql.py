#!/usr/bin/env python3
"""
SQL Converter: SQL Server to DuckDB
Converts SQL files from SQL Server syntax to DuckDB syntax using sqlglot
"""

import os
import sys
import csv
from pathlib import Path
import sqlglot
import duckdb


def replace_tokens(sql_content):
    """Replace SQL tokens with their values"""
    replacements = {
        '@scratchDatabaseSchema': 'achilles_scratch',
        '@cdmDatabaseSchema': 'cdm',
        '@tempAchillesPrefix': 'temp_achilles',
        '@schemaDelim': '.',
        '@source_name': 'Oxford',
        '@achilles_version': 'Paris-0.0.1'

    }
    
    result = sql_content
    for token, value in replacements.items():
        result = result.replace(token, value)
    
    return result


def convert_sql_file(file_path):
    """Convert a single SQL file from SQL Server to DuckDB syntax"""
    # Read the SQL file
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Replace tokens first
    sql_content = replace_tokens(sql_content)
    
    # Remove SQL Server specific hints
    lines = sql_content.split('\n')
    cleaned_lines = [line for line in lines if not line.strip().startswith('--HINT')]
    sql_content = '\n'.join(cleaned_lines)
    
    # Convert using sqlglot - let exceptions propagate
    converted = sqlglot.transpile(
        sql_content,
        read='tsql',  # SQL Server dialect
        write='duckdb',
        pretty=True
    )
    
    if not converted:
        raise ValueError(f"No conversion output generated for {file_path}")
    
    # Join all converted statements with semicolons
    return ';\n'.join(converted) + ';'


def load_analysis_details(csv_path):
    """Load analysis details from CSV and return list of analysis info where is_default=1"""
    analyses = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Only include analyses where is_default is 1
                if row.get('is_default') == '1':
                    analysis_id = row.get('analysis_id')
                    analysis_name = row.get('analysis_name', '')
                    if analysis_id:
                        analyses.append({
                            'id': analysis_id,
                            'name': analysis_name
                        })
        
        print(f"Loaded {len(analyses)} analyses with is_default=1")
        return sorted(analyses, key=lambda x: int(x['id']) if x['id'].isdigit() else 0)
        
    except Exception as e:
        print(f"ERROR: Failed to load analysis details CSV")
        print(f"Error details: {e}")
        sys.exit(1)


def execute_analyses(sql_base_dir, analyses, db_path):
    """Convert and execute SQL files for each analysis"""
    sql_path = Path(sql_base_dir)
    conn = duckdb.connect(db_path)
    
    conn.execute("SET memory_limit='10GB';")

    print(f"\n{'=' * 80}")
    print(f"Executing {len(analyses)} analyses")
    print(f"{'=' * 80}\n")
    
    executed_count = 0
    
    for analysis in analyses:
        analysis_id = analysis['id']
        analysis_name = analysis['name']
        
        # Look for SQL file in analyses subdirectory
        sql_file = sql_path / 'analyses' / f'{analysis_id}.sql'
        
        if not sql_file.exists():
            print(f"\n{'=' * 80}")
            print(f"ERROR: SQL file not found for analysis {analysis_id}: {analysis_name}")
            print(f"{'=' * 80}")
            print(f"\nExpected file: {sql_file}")
            print(f"\n{'=' * 80}")
            conn.close()
            sys.exit(1)
        
        print(f"\n[{executed_count + 1}/{len(analyses)}] Analysis {analysis_id}: {analysis_name}")
        print(f"File: {sql_file.name}")
        
        try:
            # Convert SQL file
            converted_sql = convert_sql_file(sql_file)
            
            print(converted_sql)

            conn.execute(converted_sql)
            executed_count += 1
            print(f"✓ Successfully executed")
            
        except Exception as e:
            print(f"\n{'=' * 80}")
            print(f"ERROR: Execution failed for analysis {analysis_id}: {analysis_name}")
            print(f"{'=' * 80}")
            print(f"\nError details: {e}")
            print(f"\nConverted SQL:")
            print(converted_sql if 'converted_sql' in locals() else "Failed during conversion")
            print(f"\n{'=' * 80}")
            conn.close()
            sys.exit(1)
    
    conn.close()
    
    print(f"\n{'=' * 80}")
    print(f"Successfully executed {executed_count} analyses")
    print(f"{'=' * 80}")


def initialize_database(db_path):
    """Connect to DuckDB and drop tables if they exist"""
    print(f"Connecting to DuckDB database: {db_path}")
    
    try:
        conn = duckdb.connect(db_path)
        
        # Drop tables if they exist
        print("Dropping tables if they exist...")

        conn.execute("drop schema if exists achilles_scratch cascade;")
        conn.execute("DROP TABLE IF EXISTS results.achilles_results;")
        conn.execute("DROP TABLE IF EXISTS results.achilles_analysis;")

        conn.execute("create schema achilles_scratch")

        print("Tables dropped successfully")
        
        conn.close()
        print("Database initialization complete")
        print("=" * 80)
        
    except Exception as e:
        print(f"ERROR: Failed to initialize database")
        print(f"Error details: {e}")
        sys.exit(1)


def run_merge_script(db_path):
    """Convert and execute the merge.sql script"""
    merge_file = Path('/app/merge.sql')
    
    if not merge_file.exists():
        print(f"\n{'=' * 80}")
        print(f"ERROR: merge.sql file not found")
        print(f"Expected file: {merge_file}")
        print(f"{'=' * 80}")
        sys.exit(1)
    
    print(f"\n{'=' * 80}")
    print(f"Executing merge.sql")
    print(f"{'=' * 80}\n")
    
    try:
        conn = duckdb.connect(db_path)
        
        # Convert the merge script
        converted_sql = convert_sql_file(merge_file)
        
        # Execute the converted SQL
        conn.execute(converted_sql)
        print(f"✓ Successfully executed merge.sql")
        
        # Clean up the scratch schema
        print(f"\nCleaning up achilles_scratch schema...")
        conn.execute("DROP SCHEMA IF EXISTS achilles_scratch CASCADE;")
        print(f"✓ Successfully dropped achilles_scratch schema")
        
        conn.close()
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"ERROR: Merge script execution failed")
        print(f"{'=' * 80}")
        print(f"\nError details: {e}")
        print(f"\n{'=' * 80}")
        sys.exit(1)


def main():
    # Get database name from environment variable
    database_name = os.environ.get('DATABASE_NAME')
    if not database_name:
        print("ERROR: DATABASE_NAME environment variable must be set")
        sys.exit(1)
    
    # Construct full database path
    db_path = f'/app/data/{database_name}'
    
    # Initialize database first
    initialize_database(db_path)
    
    # Load analysis details CSV
    csv_path = '/app/inst/csv/achilles/achilles_analysis_details.csv'
    print(f"\nLoading analysis details from: {csv_path}")
    analyses = load_analysis_details(csv_path)
    
    # Execute analyses
    sql_directory = '/app/inst/sql/sql_server'
    print(f"\nSQL files location: {sql_directory}")
    print(f"Source dialect: SQL Server (tsql)")
    print(f"Target dialect: DuckDB")
    
    execute_analyses(sql_directory, analyses, db_path)
    
    # Run merge script after all analyses complete
    run_merge_script(db_path)


if __name__ == '__main__':
    main()
