# SF Parser JQ Filter
# Input: The full JSON from get-execution-history
# Output: execution_index.json structure

# 1. Capture root to access .events later
. as $root |

# 2. Build an index of events by ID, propagating the Step Name through the event chain
# This ensures that even if we are deep in retries or execution branches, we know the Step Name.
reduce $root.events[] as $e (
  {}; 
  . as $acc |
  ($e.previousEventId // 0 | tostring) as $prevId |
  ($acc[$prevId]._stepName) as $inheritedName |
  
  if ($e.type | endswith("StateEntered")) then
     ($e.stateEnteredEventDetails.name) as $newName |
     . + { ($e.id|tostring): ($e + { "_stepName": $newName }) }
  else
     . + { ($e.id|tostring): ($e + { "_stepName": $inheritedName }) }
  end
) | . as $events |

# 3. Define the output object
{
  # Try to find ExecutionStarted for ARN, robustly, with fallback to argument
  stateMachineArn: ($root.events[] | select(.type=="ExecutionStarted") | .executionStartedEventDetails.stateMachineArn // $inputStateMachineArn // "UNKNOWN"),
  
  steps: [
    $root.events[] | 
    select(
      .type == "LambdaFunctionSucceeded" or 
      .type == "LambdaFunctionFailed" or 
      .type == "LambdaFunctionTimedOut" or
      .type == "TaskSucceeded" or
      .type == "TaskFailed" or 
      .type == "TaskTimedOut"
    ) | 
    . as $end |
    # Retrieve the augmented event from our index to get the propagated StepName
    $events[($end.id|tostring)] as $endNode |
    ($endNode._stepName // "UNKNOWN") as $stepName |
    $end.timestamp as $endTime |
    
    # Logic for Lambda
    if ($end.type | startswith("LambdaFunction")) then
       $events[$end.previousEventId|tostring] as $prev1 |
       (
         if $prev1.type == "LambdaFunctionStarted" then 
            $events[$prev1.previousEventId|tostring] 
         else 
            $prev1 
         end
       ) as $scheduled |
       
       {
         stepName: $stepName,
         status: ($end.type | sub("LambdaFunction";"")),
         type: "lambda",
         startTime: ($scheduled.timestamp // $endTime),
         endTime: $endTime,
         resource: $scheduled.lambdaFunctionScheduledEventDetails.resource,
         executionId: $scheduled.lambdaFunctionScheduledEventDetails.resource
       }

    # Logic for Task (ECS / Glue / Batch / etc)
    elif ($end.type | startswith("Task")) then
       # Helper for walking back chain: End -> [Started?] -> Submitted -> [Started?] -> Scheduled
       
       # 1. Get Event prior to End
       $events[$end.previousEventId|tostring] as $p1 |
       
       # 2. Check if p1 is Submitted or Started
       (
         if $p1.type == "TaskSubmitted" then $p1
         elif $p1.type == "TaskStarted" then $events[$p1.previousEventId|tostring]
         else $p1 end 
       ) as $submittedCandidate |
       
       # 3. Resolve Submitted
       (
         if $submittedCandidate.type == "TaskSubmitted" then $submittedCandidate
         else null end
       ) as $submitted |
       
       # 4. Resolve Scheduled
       (
         if $submitted then
            $events[$submitted.previousEventId|tostring]
         else
            # If no submitted, maybe we had Started -> Scheduled
             if $p1.type == "TaskStarted" then $events[$p1.previousEventId|tostring]
             else $p1 end 
         end
       ) as $p2 |
       
       (
         if $p2.type == "TaskScheduled" then $p2
         elif $p2.type == "TaskStarted" then $events[$p2.previousEventId|tostring]
         else $p2 end
       ) as $scheduled |
       
       ($scheduled.taskScheduledEventDetails.resourceType) as $rType |
       ($end.type | sub("Task";"")) as $status |
       
       if $rType == "ecs" then
          ($scheduled.taskScheduledEventDetails.parameters | fromjson) as $params |
          ($submitted.taskSubmittedEventDetails.output | fromjson) as $out |
          {
             stepName: $stepName,
             status: $status,
             type: "ecs",
             startTime: ($submitted.timestamp // $scheduled.timestamp // $endTime),
             endTime: $endTime,
             clusterArn: ($params.Cluster // "UNKNOWN"),
             taskArn: ($out.TaskArn // "UNKNOWN"),
             taskId: (($out.TaskArn | split("/") | last) // "UNKNOWN")
          }
       elif $rType == "glue" then
          ($scheduled.taskScheduledEventDetails.parameters | fromjson) as $params |
          # Try to get output from Submitted, fallback to empty
          (
             try ($submitted.taskSubmittedEventDetails.output | fromjson) 
             catch {} 
          ) as $out |
          {
             stepName: $stepName,
             status: $status,
             type: "glue",
             startTime: ($submitted.timestamp // $scheduled.timestamp // $endTime),
             endTime: $endTime,
             jobName: ($params.JobName // "UNKNOWN"),
             jobRunId: ($out.JobRunId // "UNKNOWN")
          }
       elif $rType == "states" then
          # Handle nested Step Functions executions
          (
             try ($end.taskSucceededEventDetails.output | fromjson) 
             catch {} 
          ) as $out |
          {
             stepName: $stepName,
             status: $status,
             type: "step_function",
             startTime: ($submitted.timestamp // $scheduled.timestamp // $endTime),
             endTime: $endTime,
             executionArn: ($out.ExecutionArn // "UNKNOWN")
          }
       else
          {
             stepName: $stepName,
             status: $status,
             type: ($rType // "generic"),
             startTime: ($submitted.timestamp // $scheduled.timestamp // $endTime),
             endTime: $endTime,
             resource: ($scheduled.taskScheduledEventDetails.resource // "UNKNOWN")
          }
       end
    else
       empty
    end
  ]
}
