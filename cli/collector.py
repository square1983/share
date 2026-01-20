import argparse
import json
import logging
import os
import subprocess
import boto3
import sys
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_s3_client():
    return boto3.client('s3')

def sync_from_s3(s3_client, bucket, prefix, local_dir):
    """Downloads existing files from S3 to local directory (Resume capability)."""
    logger.info(f"Syncing from S3: s3://{bucket}/{prefix} -> {local_dir}")
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Remove prefix to get relative path
                    rel_path = os.path.relpath(key, prefix)
                    local_path = os.path.join(local_dir, rel_path)
                    
                    # Ensure subdir exists
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    
                    # Check if file exists and size matches (simple check)
                    if os.path.exists(local_path):
                        if os.path.getsize(local_path) == obj['Size']:
                            continue # Skip if already exists
                        
                    logger.info(f"Downloading {key}...")
                    s3_client.download_file(bucket, key, local_path)
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

def collect_metrics(args):
    ids = args.ids.split(',')
    execution_arn = args.execution_arn
    s3_url = args.s3_url
    
    # Parse S3 URL
    if not s3_url.startswith("s3://"):
        logger.error("S3 URL must start with s3://")
        return
    
    s3_parts = s3_url[5:].split('/', 1)
    bucket_name = s3_parts[0]
    s3_prefix = s3_parts[1] if len(s3_parts) > 1 else ""
    
    # Setup clients
    s3_client = setup_s3_client()
    cw_client = boto3.client('cloudwatch')
    logs_client = boto3.client('logs')
    glue_client = boto3.client('glue')
    
    for work_id in ids:
        work_id = work_id.strip()
        if not work_id: continue
        
        logger.info(f"Processing ID: {work_id}")
        
        # Setup directories
        base_dir = f"work/{work_id}"
        os.makedirs(base_dir, exist_ok=True)
        metrics_dir = os.path.join(base_dir, "metrics")
        os.makedirs(metrics_dir, exist_ok=True)
        
        # 1. Sync from S3 (Resume)
        # S3 Path: s3://bucket/prefix/work_id/
        current_s3_prefix = f"{s3_prefix}/{work_id}".strip('/')
        sync_from_s3(s3_client, bucket_name, current_s3_prefix, base_dir)
        
        # 2. Get Execution History
        history_file = os.path.join(base_dir, "history.json")
        if not os.path.exists(history_file):
            logger.info("Fetching execution history...")
            get_step_functions_history(execution_arn, history_file)
            upload_to_s3(s3_client, history_file, bucket_name, current_s3_prefix) # Upload history immediately
        
        # 3. Parse History
        index_file = os.path.join(base_dir, "index.json")
        if not os.path.exists(index_file):
            logger.info("Parsing execution history...")
            # We need SM ARN. Derive from Exec ARN
            # arn:aws:states:region:account:execution:SMName:ExecID
            # -> arn:aws:states:region:account:stateMachine:SMName
            parts = execution_arn.split(':')
            # execution is at index 5 (-3), SMName is at 6 (-2)
            # This logic is fragile but mimics sed logic in exe.sh
            # Simplest: replace :execution: with :stateMachine: and strip last part
            sm_arn = execution_arn.replace(":execution:", ":stateMachine:").rsplit(':', 1)[0]
            
            run_jq_parser(history_file, sm_arn, index_file)
            
            # Since index.json is derivative (not uploaded in original script?), skip upload?
            # Original script zips the whole folder. We should probably upload it too.
            upload_to_s3(s3_client, index_file, bucket_name, current_s3_prefix) # Upload index immediately

        # 4. Process Steps
        with open(index_file, 'r') as f:
            data = json.load(f)
            
        for step in data.get('steps', []):
            step_type = step.get('type')
            status = step.get('status')
            step_name = step.get('stepName')
            
            if status not in ['Succeeded', 'Failed', 'TimedOut']:
                continue
                
            safe_step_name = step_name.replace('/', '_').replace(':', '_').replace(' ', '_')
            
            # Unique Filename Logic
            base_filename = f"{step_type}_{safe_step_name}"
            output_file = get_unique_filename(metrics_dir, base_filename)
            
            if os.path.exists(output_file):
                logger.info(f"Skipping {step_name} (File exists: {output_file})")
                continue
            
            logger.info(f"Processing [{step_type}] {step_name} -> {output_file}")
            
            try:
                if step_type == 'lambda':
                    # Extract Metrics (Lambda Insights)
                    request_id = step.get('requestId')
                    if not request_id or request_id == "UNKNOWN":
                        request_id = step.get('resource') # Fallback
                        
                    # AWS CLI: aws logs filter-log-events ...
                    # Boto3 equivalent
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
                    # Currently no logic for time shifting ECS in original script
                    
                    # Fetch CPU
                    cpu_resp = cw_client.get_metric_statistics(
                        Namespace='ECS/ContainerInsights',
                        MetricName='CPUUtilization',
                        Dimensions=[
                            {'Name': 'TaskId', 'Value': task_id},
                            {'Name': 'ClusterName', 'Value': cluster}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=60,
                        Statistics=['Average', 'Maximum']
                    )
                    
                    # Fetch Memory
                    mem_resp = cw_client.get_metric_statistics(
                        Namespace='ECS/ContainerInsights',
                        MetricName='MemoryUtilization',
                        Dimensions=[
                            {'Name': 'TaskId', 'Value': task_id},
                            {'Name': 'ClusterName', 'Value': cluster}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=60,
                        Statistics=['Average', 'Maximum']
                    )
                    
                    # Combine structure similar to exe.sh logic
                    result = {
                        "task": step,
                        "metrics": {
                            "cpu": cpu_resp,
                            "memory": mem_resp
                        }
                    }
                    with open(output_file, 'w') as out:
                        json.dump(result, out, default=str)

                elif step_type == 'glue':
                    job_name = step.get('jobName')
                    run_id = step.get('jobRunId')
                    
                    # Get Job Run Details (to get precise start/end if available in step?)
                    # Step already has timestamps from SF parser.
                    # Original script calls get-job-run to robustly get Start/End from Glue API.
                    run_resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
                    started_on = run_resp['JobRun']['StartedOn'].isoformat()
                    completed_on = run_resp['JobRun'].get('CompletedOn')
                    if completed_on:
                        completed_on = completed_on.isoformat()
                    else:
                        completed_on = datetime.utcnow().isoformat() + "Z"
                        
                    # Adjust Time
                    adj_start = adjust_time(started_on, -10)
                    adj_end = adjust_time(completed_on, 10)
                    
                    logger.info(f"(Glue) Adjusting time: {started_on}->{completed_on} to {adj_start}->{adj_end}")
                    
                    # Fetch CPU
                    cpu_resp = cw_client.get_metric_statistics(
                        Namespace='Glue',
                        MetricName='glue.driver.cpuLoad',
                        Dimensions=[
                            {'Name': 'JobName', 'Value': job_name},
                            {'Name': 'JobRunId', 'Value': run_id},
                            {'Name': 'Type', 'Value': 'gauge'}
                        ],
                        StartTime=adj_start,
                        EndTime=adj_end,
                        Period=300,
                        Statistics=['Average', 'Maximum']
                    )
                    
                    mem_resp = cw_client.get_metric_statistics(
                        Namespace='Glue',
                        MetricName='glue.driver.memoryUsed',
                        Dimensions=[
                            {'Name': 'JobName', 'Value': job_name},
                            {'Name': 'JobRunId', 'Value': run_id},
                            {'Name': 'Type', 'Value': 'gauge'}
                        ],
                        StartTime=adj_start,
                        EndTime=adj_end,
                        Period=300,
                        Statistics=['Average', 'Maximum']
                    )
                    
                    result = {
                        "jobRun": run_resp['JobRun'],
                        "metrics": {
                            "cpuLoad": cpu_resp,
                            "memoryUsed": mem_resp
                        }
                    }
                    with open(output_file, 'w') as out:
                        json.dump(result, out, default=str)
                
                # Upload immediately
                if os.path.exists(output_file):
                    upload_to_s3(s3_client, output_file, bucket_name, current_s3_prefix)
                    
            except Exception as e:
                logger.error(f"Error processing step {step_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect Step Functions Metrics")
    parser.add_argument("ids", help="Comma-separated list of IDs (e.g. B-1,C-1)")
    parser.add_argument("execution_arn", help="Step Functions Execution ARN")
    parser.add_argument("s3_url", help="S3 Destination URL (s3://bucket/prefix)")
    
    args = parser.parse_args()
    collect_metrics(args)
