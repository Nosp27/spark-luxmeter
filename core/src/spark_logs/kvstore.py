def applications_key():
    return "applications"


def sequential_jobs_key(*, app_id):
    return f"sequential_jobs:{app_id}"


def hybrid_metric_key(*, app_id, metric_name, job_id):
    return f"hm:{app_id}:{job_id}:{metric_name}"


def time_series_processed_jobs(*, app_id, processor_id):
    return f"tspj:{app_id}:{processor_id}"


def job_group_hashes_key(*, app_id, group_hash):
    return f"job_group:{app_id}:{group_hash}"


def loaded_jobs_key(*, app_id):
    return f"jobs_loaded:{app_id}"


def anomaly_model_key(*, app_id, model_name, job_group):
    return f"ml:{app_id}:{model_name}:{job_group}"


def app_environment_key(*, app_id):
    return f"environment:{app_id}"
