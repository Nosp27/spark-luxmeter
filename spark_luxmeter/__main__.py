from spark_luxmeter.spark_logs.loaders import abc


if __name__ == "__main__":
    l = abc.MetricsLoader()
    ids = [a["ID"] for a in l.load_application_info_list()]
    our_id = ids[0]
    print("id:", our_id)
    data = l.load(our_id)
    breakpoint()