"""Helpers for time-varying send rates (e.g., to simulate temporal bursts).

These are used by `SourceNode` in `source_node.py` to pace outgoing batches
according to a (possibly time-varying) rate profile.
"""


def build_rate_profile_per_thread(rate_profile, batches_per_second, num_threads):
    """Normalize rate configuration into a per-thread phase profile.

    A `rate_profile` is a dict of the form
    `{"phases": [{"duration_s": <float>, "batches_per_second": <float>}, ...],
      "loop": <bool>}`.
    It describes how many batches per second should be sent over time,
    e.g. to simulate temporal bursts. If no `rate_profile` is given but a
    single `batches_per_second` value is provided, it is treated as one
    constant phase covering the whole stream. Returns `None` if neither is
    set, meaning batches are sent as fast as possible.

    The overall rate is split evenly across `num_threads`, since each
    thread streams to one processing node independently.
    """
    if rate_profile:
        phases = rate_profile.get("phases", [])
        loop = rate_profile.get("loop", False)
    elif batches_per_second:
        phases = [{"duration_s": float("inf"), "batches_per_second": float(batches_per_second)}]
        loop = False
    else:
        return None

    if not phases:
        return None

    return {
        "phases": [
            {
                "duration_s": phase["duration_s"],
                "batches_per_second": phase["batches_per_second"] / num_threads,
            }
            for phase in phases
        ],
        "loop": loop,
    }


def current_interval(rate_profile_per_thread, elapsed):
    """Return the seconds to wait before sending the next batch.

    Looks up the phase active at `elapsed` seconds into the stream and
    returns `1 / batches_per_second` for that phase. Returns `None` if
    `rate_profile_per_thread` is `None` or the active rate is 0/unset,
    meaning the batch should be sent without delay.
    """
    if not rate_profile_per_thread:
        return None

    phases = rate_profile_per_thread["phases"]
    total_duration = sum(phase["duration_s"] for phase in phases)

    if rate_profile_per_thread["loop"] and total_duration > 0:
        elapsed = elapsed % total_duration

    cumulative = 0
    rate = phases[-1]["batches_per_second"]
    for phase in phases:
        cumulative += phase["duration_s"]
        if elapsed < cumulative:
            rate = phase["batches_per_second"]
            break

    if not rate:
        return None
    return 1.0 / rate
