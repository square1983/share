import os
import json
import csv
import sys
import glob
import pandas as pd
from datetime import datetime

def parse_ecs_json(file_path):
    """
    Parses an ECS Metric JSON file to extract duration and CPU/Memory stats.
    Returns a dict with:
      - duration (seconds)
      - cpu_max_avg
      - mem_max_avg
      - cpu_timeseries_str (Avg/Max sorted by time)
      - mem_timeseries_str (Avg/Max sorted by time)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        metrics = {}
        
        # 1. Calculation Duration (from task.startedAt / stoppedAt)
        # Format example: "2026-01-08T02:10:00+00:00" or ISO 8601
        duration_seconds = None
        if 'task' in data:
            t = data['task']
            start_str = t.get('startedAt')
            stop_str = t.get('stoppedAt')
            
            if start_str and stop_str:
                try:
                    # Handle typical ISO format (could be with Z or +00:00)
                    start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    stop_dt = datetime.fromisoformat(stop_str.replace('Z', '+00:00'))
                    duration_seconds = (stop_dt - start_dt).total_seconds()
                except Exception as e:
                    print(f"Warning: could not parse dates in {file_path}: {e}")
        
        metrics['duration'] = duration_seconds

        # Helper to process Datapoints
        def process_datapoints(dp_list):
            if not isinstance(dp_list, list) or len(dp_list) == 0:
                return None, None
            # Sort by Timestamp
            try:
                sorted_dp = sorted(dp_list, key=lambda x: x.get('Timestamp', ''))
            except:
                sorted_dp = dp_list

            max_avg = 0.0
            series_parts = []
            
            for dp in sorted_dp:
                avg_val = dp.get('Average', 0.0)
                max_val = dp.get('Maximum', 0.0)
                
                if avg_val > max_avg:
                    max_avg = avg_val
                
                # Format "Avg/Max"
                series_parts.append(f"{avg_val:.2f}/{max_val:.2f}")
                
            return max_avg, ", ".join(series_parts)

        # 2. Extract CPU
        if 'metrics' in data and 'cpu' in data['metrics']:
            # Check if cpu itself is the dict with Datapoints or nested?
            # exe.sh: --slurpfile cpu "$TEMP_DIR/cpu.json" -> metrics: { cpu: $cpu[0] }
            # If cpu.json content is { "Datapoints": ... } then $cpu[0] is that object.
            # So data['metrics']['cpu'] should have 'Datapoints'
            cpu_obj = data['metrics']['cpu']
            cpu_dps = cpu_obj.get('Datapoints', [])
            c_max_avg, c_series = process_datapoints(cpu_dps)
            metrics['cpu_max_avg'] = c_max_avg
            metrics['cpu_series'] = c_series
        else:
             metrics['cpu_max_avg'] = None
             metrics['cpu_series'] = None

        # 3. Extract Memory
        if 'metrics' in data and 'memory' in data['metrics']:
            mem_obj = data['metrics']['memory']
            mem_dps = mem_obj.get('Datapoints', [])
            m_max_avg, m_series = process_datapoints(mem_dps)
            metrics['mem_max_avg'] = m_max_avg
            metrics['mem_series'] = m_series
        else:
             metrics['mem_max_avg'] = None
             metrics['mem_series'] = None
             
        return metrics

    except Exception as e:
        print(f"Error reading ECS file {file_path}: {e}")
        return None

def parse_glue_json(file_path):
    """
    Parses a Glue Metric JSON file to extract cpuLoad and memoryUsed averages.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        metrics = {}
        
        # Extract ExecutionTime from jobRun
        if 'jobRun' in data:
            metrics['execution_time'] = data['jobRun'].get('ExecutionTime')
            
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

def parse_lambda_json(file_path):
    """
    Parses a Lambda Insight JSON file to extract cpu, memory, and duration.
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

def main_excel(root_dir, output_file):
    # Dictionary to store data: execution_id -> { resource_name: { type: 'glue'/'lambda'/'ecs', metrics: ... } }
    all_data = {}
    
    all_glue_jobs = set()
    all_lambda_functions = set()
    all_ecs_tasks = set()

    print(f"Scanning directory: {root_dir} ...")

    # Walk through the directory tree
    for root, dirs, files in os.walk(root_dir):
        if os.path.basename(root) == 'metrics':
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

            # Process Glue files
            glue_files = glob.glob(os.path.join(root, "glue_*.json"))
            for jf in glue_files:
                filename = os.path.basename(jf)
                job_name = os.path.splitext(filename)[0]
                if job_name.startswith('glue_'):
                    job_name = job_name[5:]
                
                metrics = parse_glue_json(jf)
                if metrics:
                    all_data[execution_id][job_name] = {'type': 'glue', 'metrics': metrics}
                    all_glue_jobs.add(job_name)

            # Process Lambda files
            lambda_files = glob.glob(os.path.join(root, "lambda_*.json"))
            for jf in lambda_files:
                filename = os.path.basename(jf)
                lambda_name = os.path.splitext(filename)[0]
                if lambda_name.startswith('lambda_'):
                    lambda_name = lambda_name[7:]
                
                metrics = parse_lambda_json(jf)
                if metrics:
                    all_data[execution_id][lambda_name] = {'type': 'lambda', 'metrics': metrics}
                    all_lambda_functions.add(lambda_name)

            # Process ECS files
            ecs_files = glob.glob(os.path.join(root, "ecs_*.json"))
            for jf in ecs_files:
                filename = os.path.basename(jf)
                task_name = os.path.splitext(filename)[0]
                if task_name.startswith('ecs_'):
                    task_name = task_name[4:]
                
                metrics = parse_ecs_json(jf)
                if metrics:
                     all_data[execution_id][task_name] = {'type': 'ecs', 'metrics': metrics}
                     all_ecs_tasks.add(task_name)

    # Sort names
    sorted_glue = sorted(list(all_glue_jobs))
    sorted_lambda = sorted(list(all_lambda_functions))
    sorted_ecs = sorted(list(all_ecs_tasks))
    
    print(f"Found {len(sorted_glue)} Glue jobs, {len(sorted_lambda)} Lambda functions, {len(sorted_ecs)} ECS tasks.")
    print(f"Found {len(all_data)} executions.")

    # Prepare DataFrame rows
    rows = []
    sorted_executions = sorted(all_data.keys())
    
    for exec_id in sorted_executions:
        row_data = {'ExecutionId': exec_id}
        exec_data = all_data[exec_id]
        
        # Glue Data
        for gname in sorted_glue:
            item = exec_data.get(gname)
            if item and item['type'] == 'glue':
                m = item['metrics']
                row_data[f"Glue_{gname}_cpu_avg"] = m.get('cpu_load_avg')
                row_data[f"Glue_{gname}_mem_avg"] = m.get('memory_used_avg')
                row_data[f"Glue_{gname}_exec_time"] = m.get('execution_time')
            else:
                row_data[f"Glue_{gname}_cpu_avg"] = None
                row_data[f"Glue_{gname}_mem_avg"] = None
                row_data[f"Glue_{gname}_exec_time"] = None
        
        # Lambda Data
        for lname in sorted_lambda:
            item = exec_data.get(lname)
            if item and item['type'] == 'lambda':
                m = item['metrics']
                row_data[f"Lambda_{lname}_cpu_total"] = m.get('cpu_total_time')
                row_data[f"Lambda_{lname}_mem_util"] = m.get('memory_utilization')
                row_data[f"Lambda_{lname}_duration"] = m.get('duration')
            else:
                row_data[f"Lambda_{lname}_cpu_total"] = None
                row_data[f"Lambda_{lname}_mem_util"] = None
                row_data[f"Lambda_{lname}_duration"] = None
                
        # ECS Data
        for ename in sorted_ecs:
            item = exec_data.get(ename)
            if item and item['type'] == 'ecs':
                m = item['metrics']
                row_data[f"ECS_{ename}_Duration"] = m.get('duration')
                row_data[f"ECS_{ename}_CPU_MaxAvg"] = m.get('cpu_max_avg')
                row_data[f"ECS_{ename}_Mem_MaxAvg"] = m.get('mem_max_avg')
                row_data[f"ECS_{ename}_CPU_TimeSeries"] = m.get('cpu_series')
                row_data[f"ECS_{ename}_Mem_TimeSeries"] = m.get('mem_series')
            else:
                row_data[f"ECS_{ename}_Duration"] = None
                row_data[f"ECS_{ename}_CPU_MaxAvg"] = None
                row_data[f"ECS_{ename}_Mem_MaxAvg"] = None
                row_data[f"ECS_{ename}_CPU_TimeSeries"] = None
                row_data[f"ECS_{ename}_Mem_TimeSeries"] = None
        
        # Append to rows list
        rows.append(row_data)

    df = pd.DataFrame(rows)
    
    if not df.empty:
        # Reorder columns to have ExecutionId first
        cols = ['ExecutionId'] + [c for c in df.columns if c != 'ExecutionId']
        df = df[cols]
    
    # Write to Excel
    try:
        df.to_excel(output_file, index=False)
        print(f"Excel successfully written to: {output_file}")
    except Exception as e:
        print(f"Error writing to Excel: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python metrics.py <work_directory> [output.xlsx]")
        sys.exit(1)
        
    work_dir = sys.argv[1]
    out_file = sys.argv[2] if len(sys.argv) > 2 else "metrics_result.xlsx"
    
    main_excel(work_dir, out_file)
