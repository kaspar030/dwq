#!/usr/bin/env python3
import json
import sys

from dwq import Job, Disque

def main():
    Disque.connect(["localhost:7711"])

    queues = sys.argv[1] or "default"
    try:
        while True:
            jobs = Job.wait(queues, count=16)
            for job in jobs:
                print(json.dumps(job, sort_keys=True, indent=4))
    except KeyboardInterrupt:
        pass

if __name__=="__main__":
    main()
