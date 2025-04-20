"""
Documentation loaders and helpers for DuckDB.
"""

import importlib.resources
import logging
import os
import pkgutil
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("duckdb-mcp-server.resources.docs")

# Cache for loaded documentation
_docs_cache: Dict[str, str] = {}


def get_friendly_sql_docs() -> str:
    """
    Get DuckDB friendly SQL documentation.

    Returns:
        Documentation string
    """
    return _load_doc_resource("duckdb_friendly_sql.xml")


def get_data_import_docs() -> str:
    """
    Get DuckDB data import documentation.

    Returns:
        Documentation string
    """
    return _load_doc_resource("duckdb_data_import.xml")


def get_visualization_docs() -> str:
    """
    Get DuckDB data visualization documentation.
    
    Returns:
        Documentation string
    """
    return _load_doc_resource("duckdb_visualization.xml")


def _load_doc_resource(filename: str) -> str:
    """
    Load a documentation resource file.
    
    Args:
        filename: Resource filename
        
    Returns:
        File contents as string
    """
    # Check cache first
    if filename in _docs_cache:
        return _docs_cache[filename]
    
    try:
        # Try to load from package resources
        resource_path = Path(__file__).parent / "xml" / filename
        
        if resource_path.exists():
            with open(resource_path, "r", encoding="utf-8") as f:
                content = f.read()
        else:
            raise FileNotFoundError(f"Documentation resource {filename} not found")
            
        # Cache the content
        _docs_cache[filename] = content
        return content
        
    except Exception as e:
        logger.error(f"Error loading documentation resource {filename}: {str(e)}")
        # Return fallback if loading fails
        return 

    """
    Get fallback documentation when resource files are not available.
    
    Args:
        filename: Resource filename
        
    Returns:
        Fallback documentation content
    """
    if filename == "duckdb_friendly_sql.xml":
        return """
<duckdb_friendly_sql>
  <metadata>
    <title>DuckDB Friendly SQL Features</title>
    <description>Comprehensive reference of DuckDB's SQL extensions and syntactic sugar that make queries more concise, readable and powerful.</description>
  </metadata>

  <feature id="select_exclude">
    <n>SELECT * EXCLUDE</n>
    <syntax>SELECT * EXCLUDE (column1, column2) FROM table</syntax>
    <description>Returns all columns except those specified, avoiding the need to list all desired columns.</description>
    <example>
      <sql>SELECT * EXCLUDE (column1, column2) FROM table;</sql>
      <explanation>Returns all columns from the table except for 'column1' and 'column2'.</explanation>
    </example>
  </feature>

  <feature id="select_replace">
    <n>SELECT * REPLACE</n>
    <syntax>SELECT * REPLACE (expression1 AS column1, expression2 AS column2) FROM table</syntax>
    <description>Returns all columns, replacing specified columns with modified expressions.</description>
    <example>
      <sql>SELECT * REPLACE (column1 + 1 AS column1) FROM table;</sql>
      <explanation>Returns all columns from the table, but replaces 'column1' with 'column1 + 1'.</explanation>
    </example>
  </feature>

  <feature id="group_by_all">
    <n>GROUP BY ALL</n>
    <syntax>SELECT ... FROM ... GROUP BY ALL</syntax>
    <description>Automatically groups by all non-aggregated columns in the SELECT clause, reducing query verbosity and chance of errors.</description>
    <example>
      <sql>SELECT a, b, COUNT(*) FROM table GROUP BY ALL;</sql>
      <explanation>The GROUP BY ALL automatically includes all non-aggregated columns (a and b) without having to list them explicitly.</explanation>
    </example>
  </feature>

  <feature id="order_by_all">
    <n>ORDER BY ALL</n>
    <syntax>SELECT ... FROM ... ORDER BY ALL</syntax>
    <description>Orders by all columns in the SELECT clause from left to right, ensuring deterministic results.</description>
    <example>
      <sql>SELECT a, b FROM table ORDER BY ALL;</sql>
      <explanation>Orders results by all selected columns from left to right. Also supports ORDER BY ALL DESC to reverse the sort order.</explanation>
    </example>
  </feature>

  <feature id="from_first">
    <n>FROM-First Syntax</n>
    <syntax>FROM table [SELECT columns]</syntax>
    <description>Allows writing queries starting with FROM instead of SELECT, aligning with the logical order of execution.</description>
    <example>
      <sql>FROM my_table SELECT column1, column2;</sql>
      <explanation>Starts the query with the FROM clause, which matches the logical order of query execution.</explanation>
    </example>
  </feature>

  <feature id="column_aliases">
    <n>Column aliases in WHERE/GROUP BY/HAVING</n>
    <syntax>SELECT expression AS alias FROM table WHERE alias = value</syntax>
    <description>Allows using column aliases defined in SELECT in other clauses (WHERE, GROUP BY, HAVING), eliminating need for subqueries.</description>
    <example>
      <sql>SELECT a + b AS sum FROM table WHERE sum > 10;</sql>
      <explanation>Uses column alias 'sum' in WHERE clause.</explanation>
    </example>
  </feature>

  <feature id="reusable_column_aliases">
    <n>Reusable Column Aliases</n>
    <syntax>SELECT expr1 AS alias1, func(alias1) AS alias2 FROM table</syntax>
    <description>Allows using column aliases defined earlier in the same SELECT statement for subsequent columns.</description>
    <example>
      <sql>SELECT a + b AS sum, sum * 2 AS doubled FROM table;</sql>
      <explanation>Defines 'sum', then uses it to define 'doubled', all in the same SELECT statement.</explanation>
    </example>
  </feature>
</duckdb_friendly_sql>
"""
    elif filename == "duckdb_data_import.xml":
        return """
<duckdb_data_import>
  <metadata>
    <title>DuckDB Data Import Reference</title>
    <description>Comprehensive reference for importing data from various sources into DuckDB</description>
  </metadata>

  <data_source type="s3">
    <n>S3 API Support</n>
    <description>The httpfs extension supports reading, writing, and globbing files on object storage servers using the S3 API.</description>
    
    <usage>
      <example>
        <code>
FROM read_parquet('s3://bucket-name/path/to/file.parquet');
        </code>
      </example>
    </usage>
    
    <features>
      <feature name="partial_reading">
        <description>For Parquet files, DuckDB supports partial reading, using HTTP range requests to only download needed parts of the file.</description>
      </feature>
      
      <feature name="multiple_files">
        <description>Reading multiple files at once</description>
        <example>
          <code>
SELECT *
FROM read_parquet([
    's3://bucket-name/file1.parquet',
    's3://bucket-name/file2.parquet'
]);
          </code>
        </example>
      </feature>
      
      <feature name="globbing">
        <description>Allows using filesystem-like glob patterns to match multiple files</description>
        <example>
          <code>
-- Matches all files with the Parquet extension
SELECT *
FROM read_parquet('s3://bucket-name/*.parquet');
          </code>
        </example>
      </feature>
    </features>
  </data_source>

  <data_source type="csv">
    <n>CSV Import</n>
    <description>CSV loading, i.e., importing CSV files to the database, is a very common, and yet surprisingly tricky, task.</description>
    
    <examples>
      <example>
        <n>Read a CSV file, auto-infer options</n>
        <code>
FROM 'file.csv';
        </code>
      </example>
      
      <example>
        <n>Use the read_csv function with custom options</n>
        <code>
FROM read_csv('file.csv',
    delim = '|',
    header = true,
    columns = {
        'Date': 'DATE',
        'Name': 'VARCHAR',
        'Value': 'DOUBLE'
    });
        </code>
      </example>
    </examples>
    
    <auto_detection>
      <description>The DuckDB CSV reader can automatically infer which configuration flags to use by analyzing the CSV file using the CSV sniffer.</description>
    </auto_detection>
  </data_source>

  <data_source type="parquet">
    <n>Parquet Support</n>
    <description>DuckDB has excellent support for Parquet files, offering efficient reading with projection and filter pushdown.</description>
    
    <examples>
      <example>
        <n>Direct query</n>
        <code>
SELECT * FROM 'file.parquet';
        </code>
      </example>
      
      <example>
        <n>Using read_parquet</n>
        <code>
SELECT * FROM read_parquet('file.parquet');
        </code>
      </example>
      
      <example>
        <n>Multiple files</n>
        <code>
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet']);
        </code>
      </example>
      
      <example>
        <n>Get metadata</n>
        <code>
SELECT * FROM parquet_metadata('file.parquet');
        </code>
      </example>
    </examples>
  </data_source>

  <data_source type="json">
    <n>JSON Support</n>
    <description>DuckDB can read JSON data from files.</description>
    
    <examples>
      <example>
        <n>Using read_json_auto</n>
        <code>
SELECT * FROM read_json_auto('file.json');
        </code>
      </example>
      
      <example>
        <n>Working with nested JSON</n>
        <code>
SELECT json_extract(data, '$.key.nested') FROM read_json_auto('file.json');
        </code>
      </example>
    </examples>
  </data_source>
</duckdb_data_import>
"""
    elif filename == "duckdb_visualization.xml":
        return """
<duckdb_visualization>
  <metadata>
    <title>DuckDB Data Visualization Guidelines</title>
    <description>Guidelines and best practices for visualizing data from DuckDB queries</description>
  </metadata>

  <visualization_types>
    <type id="time_series">
      <name>Time Series Charts</name>
      <description>Line charts showing data points over time, ideal for temporal trends.</description>
      <suitable_for>
        <data_type>Numeric values with timestamp/date columns</data_type>
        <analysis>Trends, patterns, seasonality, anomalies over time</analysis>
      </suitable_for>
      <query_pattern>
        <code>
SELECT 
  time_column::DATE as date,
  AVG(metric_column) as avg_value
FROM 
  'data_source'
WHERE 
  time_column BETWEEN start_date AND end_date
GROUP BY 
  date
ORDER BY 
  date
        </code>
      </query_pattern>
      <best_practices>
        <practice>Consider appropriate time granularity (hour, day, month)</practice>
        <practice>Use date_trunc() for time bucketing</practice>
        <practice>Filter for relevant time periods</practice>
      </best_practices>
    </type>
    
    <type id="bar_chart">
      <name>Bar Charts</name>
      <description>Visual comparison of categorical data using rectangular bars.</description>
      <suitable_for>
        <data_type>Categorical columns with associated numeric values</data_type>
        <analysis>Comparisons, rankings, distributions by category</analysis>
      </suitable_for>
      <query_pattern>
        <code>
SELECT 
  category_column,
  SUM(metric_column) as total_value
FROM 
  'data_source'
GROUP BY 
  category_column
ORDER BY 
  total_value DESC
LIMIT 10
        </code>
      </query_pattern>
      <best_practices>
        <practice>Limit to top N categories to avoid cluttered visuals</practice>
        <practice>Consider horizontal bars for long category names</practice>
        <practice>Use appropriate aggregation (SUM, AVG, COUNT)</practice>
      </best_practices>
    </type>
    
    <type id="scatter_plot">
      <name>Scatter Plots</name>
      <description>Shows the relationship between two numeric variables.</description>
      <suitable_for>
        <data_type>Two or more numeric columns</data_type>
        <analysis>Correlations, patterns, clusters, outliers</analysis>
      </suitable_for>
      <query_pattern>
        <code>
SELECT 
  numeric_column1,
  numeric_column2,
  optional_category_column
FROM 
  'data_source'
WHERE 
  numeric_column1 IS NOT NULL AND
  numeric_column2 IS NOT NULL
LIMIT 1000
        </code>
      </query_pattern>
      <best_practices>
        <practice>Include color dimension for additional insights</practice>
        <practice>Consider adding trend lines</practice>
        <practice>Limit point count for performance</practice>
      </best_practices>
    </type>
    
    <type id="heatmap">
      <name>Heatmaps</name>
      <description>Color-coded matrix representation of data values.</description>
      <suitable_for>
        <data_type>Two categorical dimensions with a numeric measure</data_type>
        <analysis>Patterns, concentrations, variations across categories</analysis>
      </suitable_for>
      <query_pattern>
        <code>
SELECT 
  category1,
  category2,
  COUNT(*) as frequency
FROM 
  'data_source'
GROUP BY 
  category1, category2
ORDER BY
  category1, category2
        </code>
      </query_pattern>
      <best_practices>
        <practice>Use appropriate color scale</practice>
        <practice>Consider log scale for skewed data</practice>
        <practice>Sort axes meaningfully</practice>
      </best_practices>
    </type>
  </visualization_types>

  <advanced_techniques>
    <technique id="combining_visualizations">
      <name>Dashboard Composition</name>
      <description>Combining multiple visualization types for comprehensive insights.</description>
      <example>
        <steps>
          <step>Time series of overall metrics</step>
          <step>Bar chart of top categories</step>
          <step>Heatmap showing detailed breakdown</step>
        </steps>
      </example>
    </technique>
    
    <technique id="interactive_filtering">
      <name>Interactive Filtering</name>
      <description>Enabling exploration through dynamic query modification.</description>
      <implementation>
        <approach>Generate parameterized queries that can be modified by user input</approach>
      </implementation>
    </technique>
  </advanced_techniques>
</duckdb_visualization>
"""
    elif filename == "duckdb_s3_integration.xml":
        return """
<duckdb_s3_integration>
  <metadata>
    <title>DuckDB S3 Integration</title>
    <description>Comprehensive documentation on working with S3 data in DuckDB</description>
  </metadata>

  <authentication>
    <section id="secrets_auth">
      <name>Secrets-Based Authentication</name>
      <description>The preferred method for authenticating to S3 endpoints is using DuckDB's secrets functionality.</description>
      
      <method id="credential_chain">
        <name>Using credential_chain Provider</name>
        <description>Automatically fetches credentials using mechanisms provided by the AWS SDK.</description>
        <example>
          <code>
CREATE OR REPLACE SECRET mcp_s3_secret (
    TYPE s3,
    PROVIDER credential_chain
);
          </code>
        </example>
        <notes>
          <note>Tries available credential sources in order (environment, config, instance profiles)</note>
          <note>Most convenient for automatic credential discovery</note>
        </notes>
      </method>
      
      <method id="config_provider">
        <name>Using config Provider</name>
        <description>Manually specify credentials in a secret.</description>
        <example>
          <code>
CREATE OR REPLACE SECRET mcp_s3_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'YOUR_ACCESS_KEY',
    SECRET 'YOUR_SECRET_KEY',
    REGION 'us-east-1'
);
          </code>
        </example>
        <notes>
          <note>More explicit but requires managing credentials in your code</note>
          <note>Useful for specific access patterns or testing</note>
        </notes>
      </method>
    </section>
  </authentication>
  
  <operations>
    <operation id="read">
      <name>Reading from S3</name>
      <description>Reading files directly from S3 buckets.</description>
      <examples>
        <example>
          <name>Basic Read</name>
          <code>
SELECT *
FROM 's3://bucket-name/file.parquet';
          </code>
        </example>
        
        <example>
          <name>Using read_parquet</name>
          <code>
SELECT *
FROM read_parquet('s3://bucket-name/file.parquet');
          </code>
        </example>
        
        <example>
          <name>Reading Multiple Files</name>
          <code>
SELECT *
FROM read_parquet([
    's3://bucket-name/file1.parquet',
    's3://bucket-name/file2.parquet'
]);
          </code>
        </example>
        
        <example>
          <name>Using Glob Patterns</name>
          <code>
SELECT *
FROM read_parquet('s3://bucket-name/folder/*.parquet');
          </code>
        </example>
      </examples>
      
      <best_practices>
        <practice>Use filter pushdown when possible to minimize data transfer</practice>
        <practice>Consider partition pruning with well-structured data</practice>
        <practice>Use filename option to track source files: read_parquet('s3://...', filename=true)</practice>
      </best_practices>
    </operation>
    
    <operation id="metadata">
      <name>Retrieving Metadata</name>
      <description>Examining file metadata without loading full data.</description>
      <examples>
        <example>
          <name>Parquet Metadata</name>
          <code>
SELECT *
FROM parquet_metadata('s3://bucket-name/file.parquet');
          </code>
        </example>
        
        <example>
          <name>Schema Inspection</name>
          <code>
DESCRIBE SELECT * FROM 's3://bucket-name/file.parquet' LIMIT 0;
          </code>
        </example>
      </examples>
    </operation>
  </operations>
  
  <performance>
    <tip id="partial_loading">
      <name>Partial Loading</name>
      <description>DuckDB can read only required portions of files from S3.</description>
      <example>
        <code>
-- Only reads the columns needed and pushes down the filter
SELECT timestamp, user_id
FROM 's3://bucket-name/large_file.parquet'
WHERE timestamp > '2023-01-01';
        </code>
      </example>
    </tip>
    
    <tip id="parallel_execution">
      <name>Parallel Execution</name>
      <description>DuckDB parallelizes reading from S3 for better performance.</description>
      <notes>
        <note>Multiple files are read in parallel automatically</note>
        <note>Large files can be split and processed in parallel</note>
      </notes>
    </tip>
  </performance>
</duckdb_s3_integration>
"""
    else:
        return f"Documentation for {filename} not found."