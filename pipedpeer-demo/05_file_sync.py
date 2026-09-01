# File-sync proof: this job runs on a worker (--remote), mutates the
# workspace THERE, and pipedpeer syncs every change back to wherever the
# command was typed — creations, modifications, and deletions alike.
# Deletions are scoped to files this job's upload shipped out, so nothing
# unrelated on the submitting machine can ever be removed.
import os
import time

_T0 = time.monotonic()

marker = "touched by job at %s" % time.strftime("%Y-%m-%d %H:%M:%S")

with open("sync_note.txt", "a") as f:
    f.write(marker + "\n")
print("SYNC updated: sync_note.txt")

with open("created_by_job.txt", "w") as f:
    f.write(marker + "\n")
print("SYNC created: created_by_job.txt")

if os.path.exists("delete_me.txt"):
    os.remove("delete_me.txt")
print("SYNC deleted: delete_me.txt")

print("last line now:", open("sync_note.txt").read().strip().splitlines()[-1])

print(f"TOTAL {time.monotonic() - _T0:.1f}s")
