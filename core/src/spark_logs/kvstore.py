def applications_key():
    return "applications"


def sequential_jobs_key(*, app_id):
    return f"sequential_jobs:{app_id}"


def hybrid_metric_key(*, app_id, metric_name, job_id):
    return f"hm:{app_id}:{job_id}:{metric_name}"


def latest_processed_job_id_key(*, app_id):
    return f"last_job_score:{app_id}"


def job_group_hashes_key(*, app_id, group_hash):
    return f"job_group:{app_id}:{group_hash}"
