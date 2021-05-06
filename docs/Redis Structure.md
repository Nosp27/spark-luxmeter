# Redis
## Storage structure

### Raw spark data
```
{application_id}:     
    zset(JSON(application_data), score=round_execution_timestamp)
```

- `round_execution_timestamp`: when metrics fetch round starts. same among all jobs loaded 
within one round 

```
{applications}:
    zset(application_id, score=lastUpdated)
```

```
{application_id:completed_jobs}:
    zset(jobs, score=completionTime)
```


### Processed data
```
{application_id:stage_id:name_of_test}: boolean
```

- `name_of_test`: name of the test, for example `skew_test`


### Job Groups
```
{job_group:<app_id>:<job_group_hash>}: job_group_name

{last_job_score:<app_id>}: last_job_score
```
