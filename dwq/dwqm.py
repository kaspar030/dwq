#!/usr/bin/env python3
from dwq import Job, Disque

def main():
    Disque.connect(["localhost:7711"])

    qstat = Disque.qstat()
    for name in sorted(qstat.keys()):
        queue = qstat[name]
        print("name:", name, "len:", queue['len'], "blocked:", queue['blocked'])
