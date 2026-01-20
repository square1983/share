import argparse
import json
import logging
import os
import subprocess
import boto3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# You can edit this list directly if you prefer not to use arguments
DEFAULT_IDS = [
    "B-1", 
    "C-1"
]
# ---------------------

def setup_s3_client():
    return boto3.client('s3')

def sync_from_s3(s3_client, bucket, prefix, local_dir, max_workers=10):
    """Downloads existing files from S3 to local directory (Resume capability) using multi-threading."""
    logger.info(f"Syncing from S3: s3://{bucket}/{prefix} -> {local_dir}")
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        
    tasks = []
    
    def download_single_file(bucket, key, local_path, size):
        try:
            # Check if file exists and size matches
            if os.path.exists(local_path):
                if os.path.getsize(local_path) == size:
                    return # Skip
            
            # logger.info(f"Downloading {key}...") # Too verbose for many files
            s3_client.download_file(bucket, key, local_path)
        except Exception as e:
            logger.error(f"Failed to download {key}: {e}")

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Remove prefix to get relative path
                        rel_path = os.path.relpath(key, prefix)
                        local_path = os.path.join(local_dir, rel_path)
                        
                        # Ensure subdir exists (thread-safe enough for makedirs exist_ok=True)
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        
                        tasks.append(executor.submit(download_single_file, bucket, key, local_path, obj['Size']))
            
            # Wait for all
            if tasks:
                logger.info(f"Queued {len(tasks)} files for download...")
                for future in as_completed(tasks):
                    future.result() 

    except Exception as e:
        logger.error(f"Failed to sync from S3: {e}")

def upload_to_s3(s3_client, file_path, bucket, prefix):
    """Uploads a single file to S3 immediately."""
    try:
        file_name = os.path.basename(file_path)
        key = f"{prefix}/metrics/{file_name}"
        logger.info(f"Uploading {file_path} -> s3://{bucket}/{key}")
        s3_client.upload_file(file_path, bucket, key)
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")

def adjust_time(iso_time, shift_minutes):
    """
    Adjusts ISO 8601 time by shift_minutes. 
    Always outputs UTC (Z) format to avoid timezone issues.
    Robust against different input formats (Z, +09:00, etc).
    """
    try:
        # Handle "Z" -> "+0000" for fromisoformat compatibility in older python if needed
        # Python 3.11+ handles Z, but 3.7+ needs +00:00. 
        # Robust cleanup similar to shell script logic
        clean_time = iso_time.replace('Z', '+00:00')
        
        # Parse
        dt = datetime.fromisoformat(clean_time)
        
        # Adjust
        new_dt = dt + timedelta(minutes=shift_minutes)
        
        # Convert to UTC and Format as Z
        new_dt_utc = new_dt.astimezone(timezone.utc)
        return new_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception as e:
        logger.error(f"Time adjustment failed for {iso_time}: {e}")
        return iso_time # Fallback

def get_unique_filename(base_path, name):
    """
    Generates unique filename: name.json, name_01.json, name_02.json...
    """
    counter = 1
    file_path = os.path.join(base_path, f"{name}.json")
    base_name = os.path.join(base_path, name)
    
    while os.path.exists(file_path):
        file_path = f"{base_name}_{counter:02d}.json"
        counter += 1
    return file_path

def run_jq_parser(history_file, sm_arn, output_index_file):
    """Runs the jq parser via subprocess."""
    # Assuming parser.jq is in the same directory as script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jq_script = os.path.join(script_dir, "parser.jq")
    
    cmd = [
        "jq", "-f", jq_script,
        "--arg", "inputStateMachineArn", sm_arn,
        history_file
    ]
    
    with open(output_index_file, "w") as outfile:
        subprocess.check_call(cmd, stdout=outfile)

def get_step_functions_history(execution_arn, history_file):
    """Fetches execution history using AWS CLI (or boto3 if preferred, but existing script uses CLI)."""
    # Using CLI for consistency with existing robust logic, or switch to boto3?
    # Boto3 get_execution_history is paginated. CLI handles pagination if configured?
    # Actually CLI "get-execution-history" creates one big JSON? 
    # Let's use boto3 for Python native.
    
    client = boto3.client('stepfunctions')
    events = []
    paginator = client.get_paginator('get_execution_history')
    for page in paginator.paginate(executionArn=execution_arn):
        events.extend(page['events'])
        
    with open(history_file, 'w') as f:
        json.dump({'events': events}, f, default=str)

def process_execution(execution_arn, base_dir, bucket_name, relative_s3_prefix, clients):
    """
    Recursively processes a Step Functions execution.
    - execution_arn: ARN to process
    - base_dir: Local directory (e.g. work/B-1 or work/B-1/metrics/child_StepName)
    - relative_s3_prefix: S3 path suffix (e.g. prefix/B-1 or prefix/B-1/metrics/child_StepName)
    """
    s3_client = clients['s3']
    cw_client = clients['cw']
    logs_client = clients['logs']
    glue_client = clients['glue']
    
    logger.info(f"Processing Execution: {execution_arn} -> {base_dir}")
    os.makedirs(base_dir, exist_ok=True)
    metrics_dir = os.path.join(base_dir, "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    
    # 2. Get Execution History
    history_file = os.path.join(base_dir, "history.json")
    if not os.path.exists(history_file):
        # logger.info("Fetching execution history...")
        get_step_functions_history(execution_arn, history_file)
        upload_to_s3(s3_client, history_file, bucket_name, relative_s3_prefix) 
    
    # 3. Parse History
    index_file = os.path.join(base_dir, "index.json")
    if not os.path.exists(index_file):
        # logger.info("Parsing execution history...")
        sm_arn = execution_arn.replace(":execution:", ":stateMachine:").rsplit(':', 1)[0]
        run_jq_parser(history_file, sm_arn, index_file)
        upload_to_s3(s3_client, index_file, bucket_name, relative_s3_prefix) 

    # 4. Process Steps
    with open(index_file, 'r') as f:
        data = json.load(f)
    
    def process_step(step):
        step_type = step.get('type')
        status = step.get('status')
        step_name = step.get('stepName')
        
        if status not in ['Succeeded', 'Failed', 'TimedOut']:
            return
            
        safe_step_name = step_name.replace('/', '_').replace(':', '_').replace(' ', '_')
        
        # Determine Metric Output File
        base_filename = f"{step_type}_{safe_step_name}"
        output_file = get_unique_filename(metrics_dir, base_filename)
        
        # Check existence (Resume)
        if os.path.exists(output_file):
            return

        # Special handling for Step Functions (Recursion)
        if step_type == 'step_function':
            child_exec_arn = step.get('executionArn')
            if child_exec_arn and child_exec_arn != "UNKNOWN":
                logger.info(f"Found Child Execution: {step_name}")
                
                # Recursive Path: metrics/child_StepName
                child_local_dir = os.path.join(metrics_dir, f"child_{safe_step_name}")
                child_s3_prefix = f"{relative_s3_prefix}/metrics/child_{safe_step_name}"
                
                # RECURSIVE CALL
                process_execution(child_exec_arn, child_local_dir, bucket_name, child_s3_prefix, clients)
                
                # Create a placeholder file to mark this step as done?
                # Or just put metadata in output_file
                with open(output_file, 'w') as out:
                    json.dump({"type": "step_function", "childArn": child_exec_arn, "recursed": True}, out)
                upload_to_s3(s3_client, output_file, bucket_name, relative_s3_prefix)
            return

        # Normal Metrics Collection
        # logger.info(f"Processing [{step_type}] {step_name}")
        try:
            if step_type == 'lambda':
                request_id = step.get('requestId')
                if not request_id or request_id == "UNKNOWN":
                    request_id = step.get('resource')
                
                response = logs_client.filter_log_events(
                    logGroupName='/aws/lambda-insights',
                    filterPattern=f'{{ $.request_id = "{request_id}" }}'
                )
                with open(output_file, 'w') as out:
                    json.dump(response, out, default=str)
                    
            elif step_type == 'ecs':
                cluster = step.get('clusterArn')
                task_id = step.get('taskId')
                start_time = step.get('startTime')
                end_time = step.get('endTime')
                
                cpu_resp = cw_client.get_metric_statistics(
                    Namespace='ECS/ContainerInsights', MetricName='CPUUtilization',
                    Dimensions=[{'Name': 'TaskId', 'Value': task_id}, {'Name': 'ClusterName', 'Value': cluster}],
                    StartTime=start_time, EndTime=end_time, Period=60, Statistics=['Average', 'Maximum']
                )
                mem_resp = cw_client.get_metric_statistics(
                    Namespace='ECS/ContainerInsights', MetricName='MemoryUtilization',
                    Dimensions=[{'Name': 'TaskId', 'Value': task_id}, {'Name': 'ClusterName', 'Value': cluster}],
                    StartTime=start_time, EndTime=end_time, Period=60, Statistics=['Average', 'Maximum']
                )
                
                with open(output_file, 'w') as out:
                    json.dump({"task": step, "metrics": {"cpu": cpu_resp, "memory": mem_resp}}, out, default=str)

            elif step_type == 'glue':
                job_name = step.get('jobName')
                run_id = step.get('jobRunId')
                
                run_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
                started_on = run_resp['JobRun']['StartedOn'].isoformat()
                completed_on = run_resp['JobRun'].get('CompletedOn')
                if completed_on: completed_on = completed_on.isoformat()
                else: completed_on = datetime.utcnow().isoformat() + "Z"
                    
                adj_start = adjust_time(started_on, -10)
                adj_end = adjust_time(completed_on, 10)
                
                cpu_resp = cw_client.get_metric_statistics(
                    Namespace='Glue', MetricName='glue.driver.cpuLoad',
                    Dimensions=[{'Name': 'JobName', 'Value': job_name}, {'Name': 'JobRunId', 'Value': run_id}, {'Name': 'Type', 'Value': 'gauge'}],
                    StartTime=adj_start, EndTime=adj_end, Period=300, Statistics=['Average', 'Maximum']
                )
                mem_resp = cw_client.get_metric_statistics(
                    Namespace='Glue', MetricName='glue.driver.memoryUsed',
                    Dimensions=[{'Name': 'JobName', 'Value': job_name}, {'Name': 'JobRunId', 'Value': run_id}, {'Name': 'Type', 'Value': 'gauge'}],
                    StartTime=adj_start, EndTime=adj_end, Period=300, Statistics=['Average', 'Maximum']
                )
                
                with open(output_file, 'w') as out:
                    json.dump({"jobRun": run_resp['JobRun'], "metrics": {"cpuLoad": cpu_resp, "memoryUsed": mem_resp}}, out, default=str)
            
            # Upload immediately
            if os.path.exists(output_file):
                upload_to_s3(s3_client, output_file, bucket_name, relative_s3_prefix)
                
        except Exception as e:
            logger.error(f"Error processing step {step_name}: {e}")

    # Run steps in parallel
    steps = data.get('steps', [])
    with ThreadPoolExecutor(max_workers=5) as executor:
        for step in steps:
            executor.submit(process_step, step)

def process_work_id(work_id, execution_arn, bucket_name, s3_prefix, clients):
    """Processes a single Work ID (Top Level)."""
    s3_client = clients['s3']
    
    work_id = work_id.strip()
    if not work_id: return
    
    logger.info(f"Processing ID: {work_id}")
    
    # Directories
    base_dir = f"work/{work_id}"
    current_s3_prefix = f"{s3_prefix}/{work_id}".strip('/')
    
    # 1. Sync from S3 (Resume) - Only need to sync the WORK_ID root
    # This downloads history.json, index.json, and all metrics/ from previous runs
    sync_from_s3(s3_client, bucket_name, current_s3_prefix, base_dir)
    
    # 2. Start Recursive Processing
    process_execution(execution_arn, base_dir, bucket_name, current_s3_prefix, clients)
            
def collect_metrics(args):
    ids = []
    
    # Priority 1: Input File
    if args.input_file:
        try:
            with open(args.input_file, 'r') as f:
                # Read lines, strip whitespace, ignore empty lines and comments
                ids = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            logger.info(f"Loaded {len(ids)} IDs from {args.input_file}")
        except Exception as e:
            logger.error(f"Failed to read input file: {e}")
            return

    # Priority 2: Command Line Argument
    elif args.ids:
        ids = args.ids.split(',')
        
    # Priority 3: Default List in Script
    else:
        ids = DEFAULT_IDS
        logger.info(f"Using DEFAULT_IDS from script: {ids}")

    execution_arn = args.execution_arn
    s3_url = args.s3_url
    
    if not ids:
        logger.error("No Work IDs provided. Use --input-file, --ids, or edit DEFAULT_IDS.")
        return
    
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        logger.error("S3 URL must start with s3://")
        return
    
    s3_parts = s3_url[5:].split('/', 1)
    bucket_name = s3_parts[0]
    s3_prefix = s3_parts[1] if len(s3_parts) > 1 else ""
    
    # Setup clients (shared)
    clients = {
        's3': setup_s3_client(),
        'cw': boto3.client('cloudwatch'),
        'logs': boto3.client('logs'),
        'glue': boto3.client('glue')
    }
    
    # Process IDs in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for work_id in ids:
            if work_id.strip():
                futures.append(executor.submit(process_work_id, work_id, execution_arn, bucket_name, s3_prefix, clients))
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Work ID failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect Step Functions Metrics")
    
    # Mutually exclusive group for IDs not strictly necessary if we have priority logic, 
    # but let's keep it flexible.
    parser.add_argument("--ids", help="Comma-separated list of IDs (e.g. B-1,C-1)")
    parser.add_argument("--input-file", "-f", help="Path to text/csv file containing IDs (one per line)")
    
    parser.add_argument("execution_arn", help="Step Functions Execution ARN")
    parser.add_argument("s3_url", help="S3 Destination URL (s3://bucket/prefix)")
    
    args = parser.parse_args()
    collect_metrics(args)
