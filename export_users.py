#!/usr/bin/env python3
"""
Script to export all users and their roles to a CSV file using CPD API
"""

import csv
import os
from datetime import datetime
from cpd_client import CPDClient

def users_with_roles():
    """Get ALL users and their roles"""
    with CPDClient() as client:
        offset = 0
        batch_size = 100
        all_results = []
        while True:
            endpoint = "/usermgmt/v1/usermgmt/users"
            params = {"offset": offset, "limit": batch_size}
            response = client.get(endpoint, params=params)
            if response.status_code != 200:
                print(f"Error fetching users: {response.status_code} {response.text}")
                break
            data = response.json()
            print("DEBUG: user API response type:", type(data), "sample:", str(data)[:500])
            # If the response is a list, use it directly
            if isinstance(data, list):
                users = data
            elif isinstance(data, dict):
                users = data.get("users") or data.get("resources") or []
            else:
                users = []
            if not users:
                break
            all_results.extend(users)
            if len(users) < batch_size:
                break
            offset += batch_size
        return all_results

def flatten_user_record(record):
    """Flatten user record for CSV export"""
    flattened = {}
    for key, value in record.items():
        if isinstance(value, list):
            flattened[key] = '; '.join(str(v) for v in value)
        elif isinstance(value, dict):
            for subkey, subval in value.items():
                flattened[f"{key}.{subkey}"] = subval
        else:
            flattened[key] = value
    return flattened

def export_users_to_csv(records):
    """Export user records to CSV file"""
    if not records:
        print("No user records to export")
        return
    os.makedirs("exports", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exports/users_{timestamp}.csv"
    flattened_records = [flatten_user_record(record) for record in records]
    all_fieldnames = set()
    for record in flattened_records:
        all_fieldnames.update(record.keys())
    fieldnames = sorted(all_fieldnames)
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_records)
    print(f"Exported {len(records)} user records to {filename}")

def main():
    """Main execution function"""
    print("Exporting users and their roles to CSV file...")
    records = users_with_roles()
    print(f"Found {len(records)} users")
    if records:
        export_users_to_csv(records)
    print("\nExport completed!")

if __name__ == "__main__":
    main()