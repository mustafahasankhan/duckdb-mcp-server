"""
Example script to demonstrate analyzing S3 data using the DuckDB MCP server.

This example shows how to interact with the MCP server to analyze data in S3,
extract metadata, analyze data, and generate visualization suggestions.
"""

import asyncio
import json
import os
import sys
from typing import Dict, Any, List, Optional

import aiohttp


async def mcp_request(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Send a request to the MCP server.
    
    Args:
        method: MCP method name
        params: Method parameters
        
    Returns:
        Response data
    """
    # URL for the MCP server
    url = os.environ.get("MCP_SERVER_URL", "http://localhost:8000")
    
    # Prepare request data
    request_data = {
        "method": method,
        "params": params or {}
    }
    
    # Send request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=request_data) as response:
            if response.status != 200:
                raise Exception(f"Error from MCP server: {response.status}")
                
            return await response.json()


async def analyze_s3_data(s3_path: str) -> None:
    """
    Analyze data in an S3 Parquet file using the MCP server.
    
    Args:
        s3_path: S3 URL to the Parquet file
    """
    print(f"Analyzing S3 data: {s3_path}")
    
    # Step 1: Create a session
    print("\n--- Creating Session ---")
    session_result = await mcp_request("callTool", {
        "name": "create_session",
        "arguments": {}
    })
    
    session_data = json.loads(session_result["result"][0]["text"])
    session_id = session_data["session_id"]
    print(f"Created session: {session_id}")
    
    # Step 2: Analyze schema
    print("\n--- Analyzing Schema ---")
    schema_result = await mcp_request("callTool", {
        "name": "analyze_schema",
        "arguments": {
            "file_path": s3_path,
            "session_id": session_id
        }
    })
    
    schema = json.loads(schema_result["result"][0]["text"])
    print(f"Schema contains {len(schema)} columns:")
    for col in schema:
        print(f"  - {col['column_name']} ({col['column_type']})")
    
    # Step 3: Analyze data
    print("\n--- Analyzing Data ---")
    analysis_result = await mcp_request("callTool", {
        "name": "analyze_data",
        "arguments": {
            "file_path": s3_path,
            "session_id": session_id
        }
    })
    
    analysis = json.loads(analysis_result["result"][0]["text"])
    print(f"Row count: {analysis['row_count']}")
    
    if analysis["numeric_analysis"]:
        print("\nNumeric columns:")
        for col, stats in analysis["numeric_analysis"].items():
            print(f"  - {col}: min={stats['min']}, max={stats['max']}, avg={stats['avg']:.2f}")
    
    if analysis["date_analysis"]:
        print("\nDate columns:")
        for col, stats in analysis["date_analysis"].items():
            print(f"  - {col}: from {stats['min_date']} to {stats['max_date']}")
    
    # Step 4: Get visualization suggestions
    print("\n--- Visualization Suggestions ---")
    viz_result = await mcp_request("callTool", {
        "name": "suggest_visualizations",
        "arguments": {
            "file_path": s3_path,
            "session_id": session_id
        }
    })
    
    suggestions = json.loads(viz_result["result"][0]["text"])
    print(f"Got {len(suggestions)} visualization suggestions:")
    
    for i, suggestion in enumerate(suggestions[:3], 1):  # Show first 3 suggestions
        print(f"\nSuggestion {i}: {suggestion['title']}")
        print(f"Type: {suggestion['type']}")
        print(f"Description: {suggestion['description']}")
        print(f"Query: {suggestion['query'][:100]}...")
    
    # Step 5: Run a sample query based on the schema
    print("\n--- Running Sample Query ---")
    
    # Find a numeric column and categorical column for a simple analysis
    numeric_col = next(iter(analysis["numeric_analysis"].keys())) if analysis["numeric_analysis"] else None
    cat_col = next(iter(analysis["categorical_analysis"].keys())) if analysis["categorical_analysis"] else None
    
    if numeric_col and cat_col:
        query = f"""
        SELECT 
            "{cat_col}",
            COUNT(*) as count,
            AVG("{numeric_col}") as avg_value
        FROM 
            '{s3_path}'
        WHERE
            "{cat_col}" IS NOT NULL
        GROUP BY 
            "{cat_col}"
        ORDER BY 
            count DESC
        LIMIT 5
        """
        
        query_result = await mcp_request("callTool", {
            "name": "query",
            "arguments": {
                "query": query,
                "session_id": session_id
            }
        })
        
        print("Query result:")
        print(query_result["result"][0]["text"])


async def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python mcp_s3_data_analysis.py s3://bucket-name/path/to/file.parquet")
        return
    
    s3_path = sys.argv[1]
    await analyze_s3_data(s3_path)


if __name__ == "__main__":
    asyncio.run(main()) 