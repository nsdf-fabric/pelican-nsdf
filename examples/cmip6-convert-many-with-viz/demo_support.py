import contextlib
import logging
import os
import os.path
import time

import htcondor

__all__ = [
    "wait_for_job",
]


@contextlib.contextmanager
def local_chdir(path):
    """
    Temporarily changes the current working directory.

    As a context manager: 'with local_chdir(path): ...'.
    """
    oldcwd = os.getcwd()
    try:
        logging.debug("os.chdir: %s", path)
        os.chdir(path)
        yield
    finally:
        logging.debug("os.chdir: %s", oldcwd)
        os.chdir(oldcwd)


# --------------------------------------------------------------------------


def wait_for_job(job_log_file):
    """
    Returns when a "terminal" event is encountered in the job log.

    Monitors the log for up to one hour.

    Assumes that the log contains entries for only a single cluster.
    """
    while not os.path.exists(job_log_file):
        time.sleep(1)

    for event in htcondor.JobEventLog(job_log_file).events(stop_after=3600):
        e_time = event.get("EventTime", "---").replace("T", " ")

        if event.type == htcondor.JobEventType.SUBMIT:
            print(f"[{e_time}] Job submitted.")

        if event.type == htcondor.JobEventType.FILE_TRANSFER:
            if event.get("Type") == 2:
                print(f"[{e_time}] Started transferring input files.")
            elif event.get("Type") == 3:
                print(f"[{e_time}] Finished transferring input files.")
            elif event.get("Type") == 5:
                print(f"[{e_time}] Started transferring output files.")
            elif event.get("Type") == 6:
                print(f"[{e_time}] Finished transferring output files.")

        if event.type == htcondor.JobEventType.EXECUTE:
            e_host = event.get("ExecuteHost", "")
            e_host = e_host.split("alias=", maxsplit=1)

            if len(e_host) >= 2:
                e_host = e_host[1].split("&", maxsplit=1)[0]

            print(f"[{e_time}] Job executing on host {e_host}.")

        if event.type == htcondor.JobEventType.JOB_TERMINATED:
            print(f"[{e_time}] Job terminated.")
            break

        if event.type in {
            htcondor.JobEventType.JOB_ABORTED,
            htcondor.JobEventType.JOB_HELD,
            htcondor.JobEventType.CLUSTER_REMOVE,
        }:
            # Notify the user in plain language, but keep the notebook cell waiting.
            print(f"[{e_time}] Job aborted, held, or removed.")

    e_time = time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{e_time}] Done monitoring the job's log file.")
