import glob
import json
import os
import sys


def evaluate_bandwidth(log_prefix):
    search_string = os.path.join(os.path.dirname(__file__), f"logs/{log_prefix}*.log")
    relevant_logfiles = glob.glob(search_string)
    sources = []
    processing = []
    sinks = []
    for logfile in relevant_logfiles:
        start_time = None
        end_time = None
        send_bytes = 0
        received_bytes = 0
        with open(logfile) as f:
            for line in f:
                tokens = line.split("WM_LOG= ")
                if len(tokens) == 2:
                    log_infos = json.loads(tokens[1])
                    if start_time is None:
                        start_time = log_infos["start_time"]
                    end_time = log_infos["start_time"] + log_infos["duration"]
                    if "send_bytes" in log_infos:
                        send_bytes += log_infos["send_bytes"]
                    if "received_bytes" in log_infos:
                        received_bytes += log_infos["received_bytes"]
        if "_source_" in logfile:
            sources.append((start_time, end_time, send_bytes, received_bytes))
            assert "_sink_" not in logfile and "_processing_" not in logfile
        elif "_processing_" in logfile:
            processing.append((start_time, end_time, send_bytes, received_bytes))
            assert "_sink_" not in logfile and "_source_" not in logfile
        elif "_sink_" in logfile:
            sinks.append((start_time, end_time, send_bytes, received_bytes))
            assert "_source_" not in logfile and "_processing_" not in logfile

    send_bytes = sum([send_bytes for (_, _, send_bytes, _) in sources])
    start_time = min([start_time for (start_time, _, _, _) in sources])
    end_time = max([end_time for (end_time, _, _, _) in sinks])
    duration = end_time - start_time

    print(f"OVERALL_RESULTS:\n  start: {start_time}\n  duration: {duration}\n  send_bytes: {send_bytes / (1024*1024)} MiB\n  bandwidth:  {send_bytes / (duration * 1024 * 1024)} MiB/s")



if __name__ == "__main__":
    assert len(sys.argv) == 2, "USAGE: python {__file__} LOG_PREFIX"
    evaluate_bandwidth(log_prefix=sys.argv[1])