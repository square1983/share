# SF Parser JQ Filter
# Input: The full JSON from get-execution-history
# Output: execution_index.json structure

# 1. Capture root to access .events later
. as $root |

# 2. Build an index of events by ID for random access
#    We process $root.events and output the index object
reduce $root.events[] as $e ({}; . + { ($e.id|tostring): $e }) | . as $events |

# 3. Define the output object
{
  # Try to find ExecutionStarted for metadata (usually ID 1)
  stateMachineArn: ($events["1"].executionStartedEventDetails.stateMachineArn // "UNKNOWN"),
  
  # 4. Find all "Step Spans" by iterating original events
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
    $end.timestamp as $endTime |
    
    # Logic for Lambda
    if ($end.type | startswith("LambdaFunction")) then
       # Chain: End (Succeeded) -> Started -> Scheduled
       $events[$end.previousEventId|tostring] as $prev1 |
       
       # Determine which one is "Scheduled"
       (
         if $prev1.type == "LambdaFunctionStarted" then 
            $events[$prev1.previousEventId|tostring] 
         else 
            $prev1 
         end
       ) as $scheduled |
       
       # Now find StateEntered for naming (Scheduled -> StateEntered)
       $events[$scheduled.previousEventId|tostring] as $entered |
       
       {
         stepName: ($entered.stateEnteredEventDetails.name // "UNKNOWN"),
         status: ($end.type | sub("LambdaFunction";"")),
         type: "lambda",
         startTime: $scheduled.timestamp,
         endTime: $endTime,
         resource: $scheduled.lambdaFunctionScheduledEventDetails.resource,
         executionId: $scheduled.lambdaFunctionScheduledEventDetails.resource
       }

    # Logic for Task (ECS / Glue / Batch / etc)
    elif ($end.type | startswith("Task")) then
       # Chain: End (Succeeded) -> Submitted -> Scheduled
       $events[$end.previousEventId|tostring] as $submitted |
       $events[$submitted.previousEventId|tostring] as $scheduled |
       $events[$scheduled.previousEventId|tostring] as $entered |
       
       ($scheduled.taskScheduledEventDetails.resourceType) as $rType |
       ($entered.stateEnteredEventDetails.name // "UNKNOWN") as $stepName |
       ($end.type | sub("Task";"")) as $status |
       
       if $rType == "ecs" then
          ($scheduled.taskScheduledEventDetails.parameters | fromjson) as $params |
          ($submitted.taskSubmittedEventDetails.output | fromjson) as $out |
          {
             stepName: $stepName,
             status: $status,
             type: "ecs",
             startTime: $submitted.timestamp,
             endTime: $endTime,
             clusterArn: $params.Cluster,
             taskArn: $out.TaskArn,
             taskId: ($out.TaskArn | split("/") | last)
          }
       elif $rType == "glue" then
          ($scheduled.taskScheduledEventDetails.parameters | fromjson) as $params |
          ($submitted.taskSubmittedEventDetails.output | fromjson) as $out |
          {
             stepName: $stepName,
             status: $status,
             type: "glue",
             startTime: $submitted.timestamp,
             endTime: $endTime,
             jobName: $params.JobName,
             jobRunId: $out.JobRunId
          }
       else
          {
             stepName: $stepName,
             status: $status,
             type: ($rType // "generic"),
             startTime: $submitted.timestamp,
             endTime: $endTime,
             resource: $scheduled.taskScheduledEventDetails.resource
          }
       end
    else
       empty
    end
  ]
}
