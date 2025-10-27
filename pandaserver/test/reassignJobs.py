import sys

from pandaserver.userinterface import Client

if len(sys.argv) == 2:
    job_id = int(sys.argv[1])
    ret = Client.reassign_jobs([job_id])
else:
    job_id_start = int(sys.argv[1])
    job_id_end = int(sys.argv[2])
    if job_id_start > job_id_end:
        print(f"{job_id_end} is less than {job_id_start}")
        sys.exit(1)

    ret = Client.reassign_jobs(range(job_id_start, job_id_end + 1))

print(ret)
