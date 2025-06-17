import os
import csv
from dotenv import load_dotenv
from cpd_client import CPDClient
from typing import Dict, List

load_dotenv()


def create_user_payload(username: str, display_name: str, email: str) -> Dict:
    """
    Create user payload with hardcoded values and dynamic username, display_name, email
    """
    return {
        "username": username,
        "displayName": display_name,
        "email": email,
        "approval_status": "approved",
        "permissions": [
            "create_space",
            "sign_in_only",
            "create_project"
        ],
        "user_roles": [],
        "current_account_status": "enabled",
        "internal_user": False,
        "deletable": True,
        "authenticator": "external",
        "role": "User"
    }


def extract_username_from_email(email: str) -> str:
    """Extract username from email address (part before @)"""
    return email.split('@')[0] if '@' in email else email


def create_user(client: CPDClient, username: str, display_name: str, email: str) -> tuple:
    """
    Create a user in CPD
    
    Returns:
        tuple: (status_message, uid) where uid is None if creation failed
    """
    url = "/usermgmt/v1/user"
    
    payload = create_user_payload(username, display_name, email)
    
    try:
        response = client.post(url, json=payload)
        
        if response.status_code == 201:
            try:
                response_data = response.json()
                uid = response_data.get('uid', 'N/A')
                message_code = response_data.get('_messageCode_', 'success')
                print(f"✓ Successfully created user: {username} ({display_name}) - UID: {uid}")
                return f"SUCCESS: {message_code}", uid
            except:
                print(f"✓ Successfully created user: {username} ({display_name}) - UID: Unknown")
                return "SUCCESS: response parsing failed", None
        else:
            try:
                response_data = response.json()
                message_code = response_data.get('_messageCode_', 'unknown_error')
                message = response_data.get('message', response.text)
                
                if message_code == 'email_exist':
                    print(f"⚠ Email already exists: {username} ({display_name})")
                    return f"EXISTS: {message}", None
                else:
                    error_msg = f"HTTP {response.status_code} - {message_code}: {message}"
                    print(f"✗ Failed to create user {username}: {error_msg}")
                    return f"ERROR: {error_msg}", None
            except:
                error_msg = f"HTTP {response.status_code} - {response.text}"
                print(f"✗ Failed to create user {username}: {error_msg}")
                return f"ERROR: {error_msg}", None
            
    except Exception as e:
        error_msg = f"Request failed: {e}"
        print(f"✗ Error creating user {username}: {error_msg}")
        return f"ERROR: {error_msg}", None


def main(input_file: str):
    """Main execution function"""
    
    input_filename = input_file
    
    # Generate output filename based on input filename
    if input_filename.endswith('.csv'):
        output_filename = input_filename[:-4] + '_out.csv'
    else:
        output_filename = input_filename + '_out.csv'
    
    print(f"Input file: {input_filename}")
    print(f"Output file: {output_filename}")
    
    with CPDClient() as client:
        print("\n" + "="*60)
        print("PROCESSING BULK USER CREATION")
        print("="*60)
        
        # Read CSV and process each row individually
        results_data = []
        
        try:
            with open(input_filename, encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',')
                for row_num, row in enumerate(reader, 1):
                    if len(row) < 2:  # Ensure we have all required columns
                        print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                        continue
                    
                    display_name = row[0].strip()
                    email = row[1].strip()
                    
                    # Extract username from email
                    username = extract_username_from_email(email)
                    
                    print(f"\nProcessing row {row_num}: {username} ({display_name})")
                    
                    # Initialize result tracking for this row
                    result_row = {
                        'original_row': row,
                        'row_number': row_num,
                        'display_name': display_name,
                        'email': email,
                        'username': username,
                        'creation_status': '',
                        'uid': None
                    }
                    
                    # Validate email format
                    if '@' not in email or not email.strip():
                        error_msg = "Invalid email format"
                        print(f"  ✗ {error_msg}")
                        result_row['creation_status'] = f"ERROR: {error_msg}"
                        result_row['uid'] = None
                        results_data.append(result_row)
                        continue
                    
                    # Validate display name
                    if not display_name.strip():
                        error_msg = "Display name cannot be empty"
                        print(f"  ✗ {error_msg}")
                        result_row['creation_status'] = f"ERROR: {error_msg}"
                        result_row['uid'] = None
                        results_data.append(result_row)
                        continue
                    
                    try:
                        # Create user
                        creation_status, uid = create_user(client, username, display_name, email)
                        result_row['creation_status'] = creation_status
                        result_row['uid'] = uid
                        
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        result_row['creation_status'] = f"ERROR: {error_msg}"
                        result_row['uid'] = None
                    
                    results_data.append(result_row)
        
        except FileNotFoundError:
            print(f"ERROR: {input_filename} file not found")
            return
        except Exception as e:
            print(f"ERROR reading CSV file: {e}")
            return
        
        print(f"\nProcessed {len(results_data)} rows from CSV")

        print("\n" + "="*60)
        print("WRITING RESULTS CSV")
        print("="*60)
        
        # Write results CSV
        try:
            with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                header = [
                    'Display Name', 'Email', 'Username', 'UID', 'Creation Status'
                ]
                writer.writerow(header)
                
                # Write data rows
                for result_row in results_data:
                    output_row = [
                        result_row['display_name'],
                        result_row['email'],
                        result_row['username'],
                        result_row['uid'] if result_row['uid'] is not None else '',
                        result_row['creation_status']
                    ]
                    writer.writerow(output_row)
            
            print(f"Results written to: {output_filename}")
            
            # Print summary statistics
            total_rows = len(results_data)
            successful_creations = sum(1 for r in results_data if r['creation_status'] == "SUCCESS")
            existing_users = sum(1 for r in results_data if r['creation_status'].startswith("EXISTS"))
            failed_creations = sum(1 for r in results_data if r['creation_status'].startswith("ERROR"))
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"Successfully created: {successful_creations}")
            print(f"Already existing: {existing_users}")
            print(f"Failed creations: {failed_creations}")
            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print("Bulk user creation process completed.")
        print(f"Detailed results saved to: {output_filename}")


if __name__ == "__main__":
    # Input CSV format (without header):
    # Display Name,Email
    # Example:
    # John Smith,john.smith@company.com
    # Jane Doe,jane.doe@company.com
    main(input_file='users.csv')