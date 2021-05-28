def interpolate(data_json):
    whole_data = [x[0] for x in data_json]

    first_not_none_idx, first_not_none = next(
        ((idx, x) for idx, x in enumerate(whole_data) if x is not None), (None, None)
    )
    if not first_not_none:
        return data_json

    timestamps = [x[1] for x in data_json]

    for i in range(first_not_none_idx, len(whole_data)):
        if whole_data[i] is None:
            # Nearest not None on the right
            n_idx, next_value = next(
                (
                    (idx, x)
                    for idx, x in enumerate(whole_data[i + 1 :], i + 1)
                    if x is not None
                ),
                (None, None),
            )

            # Nearest not None on the left
            p_idx, prev_value = next(
                (
                    (idx, x)
                    for idx, x in enumerate(reversed(whole_data[:i]), 1)
                    if x is not None
                ),
                (None, None),
            )
            if prev_value is not None:
                p_idx = i - p_idx

            if next_value is None or prev_value is None:
                whole_data[i] = next_value or prev_value
            else:
                dtn = timestamps[n_idx] - timestamps[i]
                dtp = timestamps[i] - timestamps[p_idx]
                dts = dtn + dtp
                dtn /= dts
                dtp /= dts
                assert 0.99 < dtn + dtp <= 1, dtn + dtp
                whole_data[i] = next_value * dtn + prev_value * dtp
    return list(zip(whole_data, timestamps))
