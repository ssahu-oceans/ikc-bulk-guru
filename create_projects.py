import csv
from cpd_client import CPDClient
from typing import Dict, List, Optional, Tuple
from datetime import datetime


def extract_username_from_email(email_or_username: str) -> str:
    """Extract username from email address (part before @) or return as-is if no @"""
    return email_or_username.split('@')[0] if '@' in email_or_username else email_or_username


def check_user_exists(client: CPDClient, username: str) -> Tuple[bool, Optional[str]]:
    """
    Check if user exists in CPD and get user ID
    
    Args:
        client: CPD client
        username: Username to check
        
    Returns:
        tuple: (exists: bool, user_id: str or None)
    """
    print(f"    • Checking if user '{username}' exists...")
    
    try:
        url = f"/usermgmt/v1/user/{username}"
        response = client.get(url)
        
        if response.status_code == 200:
            try:
                data = response.json()
                user_id = data.get('uid', None)
                display_name = data.get('displayName', username)
                print(f"      ✓ User found: {display_name} (UID: {user_id})")
                return True, str(user_id) if user_id else None
            except:
                print(f"      ✓ User found (response parsing failed)")
                return True, None
        elif response.status_code == 404:
            try:
                error_data = response.json()
                message_code = error_data.get('_messageCode_', 'not_found')
                if message_code == 'not_found':
                    print(f"      ✗ User '{username}' not found")
                else:
                    print(f"      ✗ User '{username}' not found: {message_code}")
            except:
                print(f"      ✗ User '{username}' not found")
            return False, None
        else:
            print(f"      ✗ Error checking user '{username}': HTTP {response.status_code}")
            return False, None
            
    except Exception as e:
        print(f"      ✗ Error checking user '{username}': {e}")
        return False, None


def check_project_exists(client: CPDClient, project_name: str) -> Optional[str]:
    """
    Check if project exists by name
    
    Args:
        client: CPD client
        project_name: Name of the project to check
        
    Returns:
        str: Project ID if exists, None if not found
    """
    print(f"  → Checking if project '{project_name}' exists...")
    
    try:
        # Use exact match to find project by name
        url = f"/v2/projects?name={project_name}&match=exact&include=members"
        response = client.get(url)
        
        if response.status_code == 200:
            data = response.json()
            total_results = data.get('total_results', 0)
            
            if total_results > 0:
                resources = data.get('resources', [])
                if resources:
                    project_id = resources[0]['metadata']['guid']
                    print(f"    ✓ Project found with ID: {project_id}")
                    return project_id
            
            print(f"    • Project '{project_name}' not found")
            return None
        else:
            print(f"    ✗ Error checking project: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"    ✗ Error checking project: {e}")
        return None


def create_project(client: CPDClient, project_name: str, description: str = "") -> Tuple[bool, str]:
    """
    Create a new project
    
    Args:
        client: CPD client
        project_name: Name of the project to create
        description: Project description
        
    Returns:
        tuple: (success: bool, project_id_or_error: str)
    """
    print(f"  → Creating project '{project_name}'...")
    
    try:
        url = "/transactional/v2/projects"
        
        payload = {
            "name": project_name,
            "description": description if description else "A project description.",
            "generator": "DAP-Projects",
            "public": False,
            "enforce_members": False,
            "storage": {
                "type": "assetfiles"
            }
        }
        
        response = client.post(url, json=payload)
        
        if response.status_code == 201:
            data = response.json()
            location = data.get('location', '')
            
            # Extract project ID from location: "/v2/projects/092268e8-0d3f-432c-98a1-e0aaee872945"
            if location:
                project_id = location.split('/')[-1]
                print(f"    ✓ Project created successfully with ID: {project_id}")
                return True, project_id
            else:
                print(f"    ✗ Project created but no location returned")
                return False, "Project created but no ID returned"
        else:
            try:
                error_data = response.json()
                reason = error_data.get('reason', error_data.get('message', response.text))
                
                if response.status_code == 400 and 'already used' in reason:
                    print(f"    ⚠ Project name already exists: {reason}")
                    return False, f"Project already exists: {reason}"
                else:
                    print(f"    ✗ Failed to create project: {response.status_code} - {reason}")
                    return False, f"HTTP {response.status_code}: {reason}"
            except:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"    ✗ Failed to create project: {error_msg}")
                return False, error_msg
                
    except Exception as e:
        error_msg = f"Request failed: {e}"
        print(f"    ✗ Error creating project: {error_msg}")
        return False, error_msg


def get_project_members(client: CPDClient, project_id: str, project_name: str = None) -> List[str]:
    """
    Get current project members
    
    Args:
        client: CPD client
        project_id: Project ID
        project_name: Project name (optional, for better API call)
        
    Returns:
        List[str]: List of usernames that are project members
    """
    print(f"  → Getting current members for project {project_id}...")
    
    try:
        # Use project name if available for more efficient search, otherwise use project ID
        if project_name:
            url = f"/v2/projects?name={project_name}&match=exact&include=members"
        else:
            url = f"/v2/projects/{project_id}?include=members"
        
        response = client.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            if project_name:
                # Using search by name
                resources = data.get('resources', [])
                if resources:
                    members = resources[0].get('entity', {}).get('members', [])
                else:
                    return []
            else:
                # Using direct project ID (if this endpoint exists)
                members = data.get('entity', {}).get('members', [])
            
            # Extract usernames from members
            usernames = []
            for member in members:
                if member.get('type') == 'user' and member.get('state') == 'ACTIVE':
                    username = member.get('user_name')
                    if username:
                        usernames.append(username)
            
            print(f"    ✓ Found {len(usernames)} active members: {', '.join(usernames) if usernames else 'None'}")
            return usernames
        else:
            print(f"    ✗ Error getting project members: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"    ✗ Error getting project members: {e}")
        return []


def assign_collaborators(client: CPDClient, project_id: str, user_assignments: List[Dict], role: str = "editor") -> str:
    """
    Assign multiple collaborators to project
    
    Args:
        client: CPD client
        project_id: Project ID
        user_assignments: List of dicts with 'username' and 'user_id'
        role: Role to assign (default: editor)
        
    Returns:
        str: Status message (SUCCESS or ERROR with details)
    """
    if not user_assignments:
        return "No users to assign"
    
    usernames = [u['username'] for u in user_assignments]
    print(f"    • Assigning {len(user_assignments)} users as {role}: {', '.join(usernames)}")
    
    try:
        url = f"/v2/projects/{project_id}/members"
        
        # Build members payload
        members = []
        for assignment in user_assignments:
            members.append({
                "user_name": assignment['username'],
                "id": assignment['user_id'],
                "role": role
            })
        
        payload = {"members": members}
        
        response = client.post(url, json=payload)
        
        if response.status_code == 200:
            try:
                data = response.json()
                assigned_members = data.get('members', [])
                assigned_count = len(assigned_members)
                print(f"      ✓ Successfully assigned {assigned_count} users")
                return "SUCCESS"
            except:
                print(f"      ✓ Assignment successful (response parsing failed)")
                return "SUCCESS"
        else:
            try:
                error_data = response.json()
                reason = error_data.get('reason', error_data.get('message', response.text))
                
                if 'already exists in the project' in reason:
                    print(f"      ⚠ Some users already exist: {reason}")
                    return f"PARTIAL_ERROR: {reason}"
                else:
                    print(f"      ✗ Failed to assign users: HTTP {response.status_code} - {reason}")
                    return f"ERROR: HTTP {response.status_code} - {reason}"
            except:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                print(f"      ✗ Failed to assign users: {error_msg}")
                return f"ERROR: {error_msg}"
                
    except Exception as e:
        error_msg = f"Request failed: {e}"
        print(f"      ✗ Error assigning users: {error_msg}")
        return f"ERROR: {error_msg}"


def main(input_filename: str):
    """Main execution function"""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{input_filename.replace('.csv', '')}_out_{timestamp}.csv"
    
    print(f"Input file: {input_filename}")
    print(f"Output file: {output_filename}")
    
    with CPDClient() as client:
        print("\n" + "="*60)
        print("PROCESSING PROJECT AND COLLABORATOR ASSIGNMENTS")
        print("="*60)
        
        # Read CSV and process each row individually
        results_data = []
        
        try:
            with open(input_filename, encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',')
                for row_num, row in enumerate(reader, 1):
                    if len(row) < 3:  # Ensure we have minimum required columns
                        print(f"WARNING: Row {row_num} has insufficient columns, skipping")
                        continue
                    
                    project_name = row[0].strip()
                    project_description = row[1].strip() if len(row) > 1 else ""
                    collaborators = row[2].strip() if len(row) > 2 else ""
                    
                    # Parse collaborators (assuming comma-separated or pipe-separated)
                    collaborator_list = []
                    if collaborators:
                        # Handle both pipe (|) and semicolon (;) separators
                        if '|' in collaborators:
                            collaborator_list = [c.strip() for c in collaborators.split('|') if c.strip()]
                        elif ';' in collaborators:
                            collaborator_list = [c.strip() for c in collaborators.split(';') if c.strip()]
                        else:
                            # Single collaborator (no separator)
                            collaborator_list = [collaborators.strip()] if collaborators.strip() else []
                                                
                    print(f"\nProcessing row {row_num}: Project '{project_name}'")
                    
                    # Initialize result tracking for this row
                    result_row = {
                        'original_row': row,
                        'row_number': row_num,
                        'project_name': project_name,
                        'project_description': project_description,
                        'collaborators': collaborators,
                        'project_status': '',
                        'project_id': '',
                        'collaborator_results': [],
                        'overall_status': ''
                    }
                    
                    try:
                        # Step 1: Check if project exists
                        project_id = check_project_exists(client, project_name)
                        
                        if project_id:
                            print(f"  ✓ Project exists with ID: {project_id}")
                            result_row['project_status'] = "EXISTS"
                            result_row['project_id'] = project_id
                        else:
                            # Step 2: Create project if not found
                            success, project_id_or_error = create_project(client, project_name, project_description)
                            
                            if success:
                                print(f"  ✓ Project created with ID: {project_id_or_error}")
                                result_row['project_status'] = "CREATED"
                                result_row['project_id'] = project_id_or_error
                                project_id = project_id_or_error
                            else:
                                print(f"  ✗ Failed to create project: {project_id_or_error}")
                                result_row['project_status'] = f"ERROR: {project_id_or_error}"
                                result_row['overall_status'] = f"ERROR: Project creation failed"
                                results_data.append(result_row)
                                continue
                        
                        # Step 3: Validate collaborators
                        valid_collaborators = []
                        invalid_collaborators = []
                        
                        for collaborator in collaborator_list:
                            # Extract username from email if needed
                            username = extract_username_from_email(collaborator)
                            
                            print(f"    → Processing collaborator: {collaborator} (username: {username})")
                            exists, user_id = check_user_exists(client, username)
                            if exists and user_id:
                                valid_collaborators.append({
                                    'original_input': collaborator,
                                    'username': username,
                                    'user_id': user_id
                                })
                            else:
                                invalid_collaborators.append(f"{collaborator} (username: {username})")
                        
                        if invalid_collaborators:
                            print(f"  ⚠ Invalid collaborators: {', '.join(invalid_collaborators)}")
                        
                        if valid_collaborators:
                            valid_usernames = [c['username'] for c in valid_collaborators]
                            valid_inputs = [c['original_input'] for c in valid_collaborators]
                            print(f"  ✓ Valid collaborators: {', '.join(valid_inputs)} -> usernames: {', '.join(valid_usernames)}")
                            
                            # Step 4: Query Project Members
                            current_members = get_project_members(client, project_id, project_name)
                            
                            # Step 5: Filter out existing members and batch assign new ones
                            new_collaborators = []
                            existing_collaborators = []
                            
                            for collaborator_info in valid_collaborators:
                                username = collaborator_info['username']
                                if username not in current_members:
                                    new_collaborators.append(collaborator_info)
                                else:
                                    existing_collaborators.append(username)
                            
                            # Log existing members
                            if existing_collaborators:
                                print(f"    • Already members: {', '.join(existing_collaborators)}")
                                for username in existing_collaborators:
                                    # Find original input for this username
                                    original_input = next((c['original_input'] for c in valid_collaborators if c['username'] == username), username)
                                    result_row['collaborator_results'].append({
                                        'original_input': original_input,
                                        'username': username,
                                        'action': 'SKIPPED',
                                        'status': 'Already a member'
                                    })
                            
                            # Batch assign new collaborators
                            if new_collaborators:
                                assignment_result = assign_collaborators(client, project_id, new_collaborators)
                                
                                # Track results for each new collaborator
                                for collaborator_info in new_collaborators:
                                    result_row['collaborator_results'].append({
                                        'original_input': collaborator_info['original_input'],
                                        'username': collaborator_info['username'],
                                        'action': 'ASSIGNED',
                                        'status': assignment_result
                                    })
                            
                            # Set overall status
                            successful_assignments = sum(1 for r in result_row['collaborator_results'] 
                                                       if r['status'] == 'SUCCESS')
                            total_processed = len(result_row['collaborator_results'])
                            
                            result_row['overall_status'] = f"Project: {result_row['project_status']}, " \
                                                         f"Collaborators: {successful_assignments}/{total_processed} processed"
                        else:
                            result_row['overall_status'] = f"Project: {result_row['project_status']}, No valid collaborators"
                        
                    except Exception as e:
                        error_msg = f"Processing error: {e}"
                        print(f"  ✗ {error_msg}")
                        result_row['overall_status'] = f"ERROR: {error_msg}"
                    
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
                    'Project Name', 'Project Description', 'Collaborators', 
                    'Project ID', 'Project Status', 'Collaborator Details', 'Overall Status'
                ]
                writer.writerow(header)
                
                # Write data rows
                for result_row in results_data:
                    # Format collaborator results for CSV
                    collaborator_details = "; ".join([
                        f"{r['original_input']}({r['username']}):{r['action']}:{r['status']}" 
                        for r in result_row['collaborator_results']
                    ]) if result_row['collaborator_results'] else "None"
                    
                    output_row = [
                        result_row['project_name'],
                        result_row['project_description'],
                        result_row['collaborators'],
                        result_row['project_id'],
                        result_row['project_status'],
                        collaborator_details,
                        result_row['overall_status']
                    ]
                    writer.writerow(output_row)
            
            print(f"Results written to: {output_filename}")
            
            # Print summary statistics
            total_rows = len(results_data)
            successful_projects = sum(1 for r in results_data 
                                    if r['project_status'] in ['EXISTS', 'CREATED'])
            
            print(f"\nSUMMARY:")
            print(f"Total rows processed: {total_rows}")
            print(f"Successful project operations: {successful_projects}")
            print(f"Failed project operations: {total_rows - successful_projects}")
            
        except Exception as e:
            print(f"ERROR writing results CSV: {e}")

        print("\n" + "="*60)
        print("PROCESS COMPLETED")
        print("="*60)
        print("Bulk project and collaborator management process completed.")
        print(f"Detailed results saved to: {output_filename}")


if __name__ == "__main__":
    # Input CSV format (without header):
    # Project Name,Project Description,Collaborators
    # Example:
    # My Data Project,Analytics project for sales data,user1@company.com;user2@company.com
    # ML Pipeline,Machine learning project,user3@company.com;user4@company.com
    main(input_filename='projects.csv')