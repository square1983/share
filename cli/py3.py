import os
import json
import csv
import sys
import glob

def parse_glue_json(file_path):
    """
    Parses a Glue Metric JSON file to extract cpuLoad and memoryUsed averages.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        metrics = {}
        
        # Check for metrics.cpuLoad.Datapoints[0].Average
        if 'metrics' in data:
            m = data['metrics']
            
            # CPU Load
            if 'cpuLoad' in m and 'Datapoints' in m['cpuLoad']:
                dp = m['cpuLoad']['Datapoints']
                if isinstance(dp, list) and len(dp) > 0:
                    metrics['cpu_load_avg'] = dp[0].get('Average')
            
            # Memory Used
            if 'memoryUsed' in m and 'Datapoints' in m['memoryUsed']:
                dp = m['memoryUsed']['Datapoints']
                if isinstance(dp, list) and len(dp) > 0:
                    metrics['memory_used_avg'] = dp[0].get('Average')
                    
        return metrics

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def main(root_dir, output_file):
    all_data = {}
    all_job_names = set()

    print(f"Scanning directory: {root_dir} ...")

    for root, dirs, files in os.walk(root_dir):
        if os.path.basename(root) == 'metrics':
            # Extraction ID Logic (Same as aggregate_metrics.py)
            try:
                rel_path = os.path.relpath(root, root_dir)
                rel_parts = rel_path.split(os.sep)
                
                if len(rel_parts) > 0 and rel_parts[0] != '.' and rel_parts[0] != 'metrics':
                    execution_id = rel_parts[0]
                else:
                    if len(rel_parts) >= 2 and rel_parts[-1] == 'metrics':
                         execution_id = rel_parts[0]
                    else:
                         execution_id = "UNKNOWN"
            except ValueError:
                execution_id = "UNKNOWN"

            if execution_id not in all_data:
                all_data[execution_id] = {}

            # Process glue json files
            # Assuming they start with glue_ ? or just check content?
            # User's previous screenshot showed files like "glue_ファイル形式チェック.json"
            json_files = glob.glob(os.path.join(root, "glue_*.json"))
            
            for jf in json_files:
                filename = os.path.basename(jf)
                job_name = os.path.splitext(filename)[0]
                
                # Remove 'glue_' prefix
                if job_name.startswith('glue_'):
                    job_name = job_name[5:]
                
                metrics = parse_glue_json(jf)
                if metrics:
                    all_data[execution_id][job_name] = metrics
                    all_job_names.add(job_name)

    sorted_jobs = sorted(list(all_job_names))
    print(f"Found {len(sorted_jobs)} unique Glue jobs: {sorted_jobs}")
    print(f"Found {len(all_data)} executions.")

    with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        header = ['执行编号']
        for jname in sorted_jobs:
            header.append(f"{jname}_cpu_avg")
            header.append(f"{jname}_mem_avg")
        writer.writerow(header)

        sorted_executions = sorted(all_data.keys())
        
        for exec_id in sorted_executions:
            row = [exec_id]
            exec_data = all_data[exec_id]
            
            for jname in sorted_jobs:
                metrics = exec_data.get(jname, {})
                row.append(metrics.get('cpu_load_avg', ''))
                row.append(metrics.get('memory_used_avg', ''))
            
            writer.writerow(row)

    print(f"CSV successfully written to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 aggregate_glue_metrics.py <work_directory> [output.csv]")
        sys.exit(1)
        
    work_dir = sys.argv[1]
    out_csv = sys.argv[2] if len(sys.argv) > 2 else "glue_result.csv"
    
    main(work_dir, out_csv)
