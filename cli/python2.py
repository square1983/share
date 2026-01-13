import os
import json
import csv
import sys
import glob

def parse_lambda_json(file_path):
    """
    Parses a Lambda Insight JSON file to extract cpu, memory, and duration.
    Handles the nested 'message' JSON string.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Try to find the event with the message
        if 'events' in data:
            for event in data['events']:
                if 'message' in event:
                    try:
                        # Parse the nested JSON string in 'message'
                        message_data = json.loads(event['message'])
                        return {
                            'cpu_total_time': message_data.get('cpu_total_time'),
                            'memory_utilization': message_data.get('memory_utilization'),
                            'duration': message_data.get('duration')
                        }
                    except (json.JSONDecodeError, TypeError):
                        continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None
    return None

def main(root_dir, output_file):
    # Dictionary to store data: execution_id -> { lambda_name: metrics }
    all_data = {}
    # Set to keep track of all unique lambda names encountered
    all_lambda_names = set()

    print(f"Scanning directory: {root_dir} ...")

    # Walk through the directory tree
    for root, dirs, files in os.walk(root_dir):
        if os.path.basename(root) == 'metrics':
            # Found a metrics directory
            # Structure could be:
            # Type A: .../C-2/<hash>/metrics  -> ExID is at -3
            # Type B: .../C-2/metrics         -> ExID is at -2
            # Type C: .../C-2/output/metrics  -> ExID is at -3 (if output replaces hash)
            # If we find 'id' at -3 is 'output', we might need to go to -4?
            
            path_parts = root.split(os.sep)
            
            # Helper to find likely execution ID
            # We assume Execution ID is NOT 'metrics', 'output', or a long hash (unless hash is the ID)
            # Let's try to look at -3 first, if it is 'output', go to -4.
            
            candidate_id = "UNKNOWN"
            if len(path_parts) >= 3:
                val = path_parts[-3]
                if val == 'output' or val == 'sf_data' or val.startswith('sf_data_'):
                     if len(path_parts) >= 4:
                         candidate_id = path_parts[-4]
                     else:
                         candidate_id = val # Fallback
                else:
                    candidate_id = val
            elif len(path_parts) >= 2:
                candidate_id = path_parts[-2]
            
            execution_id = candidate_id

            if execution_id not in all_data:
                all_data[execution_id] = {}

            # Process all json files in this metrics directory
            json_files = glob.glob(os.path.join(root, "*.json"))
            for jf in json_files:
                filename = os.path.basename(jf)
                # Use filename without extension as lambda name
                lambda_name = os.path.splitext(filename)[0]
                
                # Remove 'lambda_' prefix if present
                if lambda_name.startswith('lambda_'):
                    lambda_name = lambda_name[7:] # remove lambda_
                
                metrics = parse_lambda_json(jf)
                if metrics:
                    all_data[execution_id][lambda_name] = metrics
                    all_lambda_names.add(lambda_name)

    # Sort lambda names to ensure consistent column order
    sorted_lambdas = sorted(list(all_lambda_names))
    print(f"Found {len(sorted_lambdas)} unique Lambda functions: {sorted_lambdas}")
    print(f"Found {len(all_data)} executions.")

    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Build Header
        header = ['执行编号']
        for lname in sorted_lambdas:
            header.append(f"{lname}_cpu_total_time")
            header.append(f"{lname}_memory_utilization")
            header.append(f"{lname}_duration")
        writer.writerow(header)

        # Build Rows
        # Sort executions by ID (try to sort numerically if possible, else string)
        sorted_executions = sorted(all_data.keys())
        
        for exec_id in sorted_executions:
            row = [exec_id]
            exec_data = all_data[exec_id]
            
            for lname in sorted_lambdas:
                metrics = exec_data.get(lname, {})
                row.append(metrics.get('cpu_total_time', ''))
                row.append(metrics.get('memory_utilization', ''))
                row.append(metrics.get('duration', ''))
            
            writer.writerow(row)

    print(f"CSV successfully written to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python aggregate_metrics.py <work_directory> [output.csv]")
        print("Example: python aggregate_metrics.py ../work result.csv")
        sys.exit(1)
        
    work_dir = sys.argv[1]
    out_csv = sys.argv[2] if len(sys.argv) > 2 else "result.csv"
    
    main(work_dir, out_csv)
