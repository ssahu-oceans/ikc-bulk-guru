#!/usr/bin/env python3
"""
Script to export all CPD artifact types including reference data to CSV files using search API
"""

import csv
import os
from datetime import datetime
from cpd_client import CPDClient


def artifacts_by_type(artifact_type):
    """Get ALL records for an artifact type"""
    with CPDClient() as client:
        offset = 0
        batch_size = 10000
        all_results = []
        
        while True:
            payload = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"metadata.artifact_type": artifact_type}},
                            {"term": {"metadata.state": "PUBLISHED"}}
                        ]
                    }
                },
                "from": offset,
                "size": batch_size,
                "_source": [
                    "metadata.name",
                    "categories.primary_category_name",
                    "entity.artifacts.global_id",
                    "artifact_id"
                ],
            }
            
            response = client.search(payload)
            if response.status_code != 200:
                print(f"Error searching {artifact_type}: {response.status_code}")
                break
                
            data = response.json()
            rows = data.get("rows", [])
            total_hits = data.get("size", 0)
            
            if not rows:
                break
                
            all_results.extend(rows)
            
            # Check if we've got all available records
            if offset + len(rows) >= total_hits:
                break
                
            offset += batch_size
        
        return all_results


def flatten_record(record):
    """Flatten nested record structure for CSV export"""
    flattened = {}
    
    def _flatten(obj, prefix=''):
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Skip the _score field as it's not useful for this export
                if key == '_score':
                    continue
                new_key = f"{prefix}.{key}" if prefix else key
                _flatten(value, new_key)
        elif isinstance(obj, list):
            if obj:
                if len(obj) == 1:
                    _flatten(obj[0], prefix)
                else:
                    flattened[prefix] = '; '.join(str(item) for item in obj)
            else:
                flattened[prefix] = ''
        else:
            flattened[prefix] = str(obj) if obj is not None else ''
    
    _flatten(record)
    return flattened


def export_to_csv(artifact_type, records):
    """Export records to CSV file"""
    if not records:
        print(f"No records to export for {artifact_type}")
        return
        
    # Create exports directory if it doesn't exist
    os.makedirs("exports", exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exports/{artifact_type}_{timestamp}.csv"
    
    # Flatten all records and collect all possible fieldnames
    flattened_records = [flatten_record(record) for record in records]
    all_fieldnames = set()
    for record in flattened_records:
        all_fieldnames.update(record.keys())
    
    # Sort fieldnames for consistent output
    fieldnames = sorted(all_fieldnames)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_records)
        
    print(f"Exported {len(records)} {artifact_type} records to {filename}")


def main():
    """Main execution function"""
    
    artifact_types = [
        "glossary_term",
        "data_class", 
        "classification",
        "reference_data"
    ]
    
    print("Exporting CPD artifacts to CSV files...")
    
    for artifact_type in artifact_types:
        print(f"\nProcessing {artifact_type}...")
        records = artifacts_by_type(artifact_type)
        print(f"Found {len(records)} {artifact_type} records")
        
        if records:
            export_to_csv(artifact_type, records)
    
    print("\nExport completed!")


if __name__ == "__main__":
    main()