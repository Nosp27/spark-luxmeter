from spark_logs.loaders import abc


if __name__ == "__main__":
    l = abc.MetricsLoader()
    ids = list(l.load_application_info_list().keys())
    our_id = ids[0]
    print("id:", our_id)
    data = l.load(our_id)
    breakpoint()
