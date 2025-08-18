#!/usr/bin/env python3
"""
Simple script to list all projects from the database.
This helps you find project names to test with.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def list_projects():
    """List all projects from the database."""
    print("📋 Listing all projects from database...")
    print("=" * 50)
    
    try:
        from utils.db import get_projects
        
        projects_df = get_projects()
        
        if projects_df.empty:
            print("❌ No projects found in database")
            return
        
        print(f"✅ Found {len(projects_df)} projects:")
        print()
        
        for idx, project in projects_df.iterrows():
            project_id = project.get('id', 'N/A')
            name = project.get('name', 'N/A')
            description = project.get('description', 'N/A')
            catalog = project.get('catalog', 'N/A')
            schema = project.get('schema', 'N/A')
            
            print(f"📁 Project {project_id}: '{name}'")
            print(f"   Description: {description}")
            print(f"   Catalog: {catalog}")
            print(f"   Schema: {schema}")
            print()
        
        print("💡 To test logged models for a project, run:")
        print("   python simple_model_test.py <project_name>")
        print()
        print("Example:")
        if not projects_df.empty:
            first_project_name = projects_df.iloc[0]['name']
            print(f"   python simple_model_test.py \"{first_project_name}\"")
        
    except Exception as e:
        print(f"❌ Error listing projects: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    list_projects()