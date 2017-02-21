#!/usr/bin/env python3
import json
import sys

from dwq import Job, Disque

def main():
    Disque.connect(["localhost:7711"])

    queues = sys.argv[1:] or ["default"]
    try:
        while True:
            jobs = Job.get(queues, count=1024, nohang=True)
            if not jobs:
                return
    except KeyboardInterrupt:
        pass

if __name__=="__main__":
    main()
