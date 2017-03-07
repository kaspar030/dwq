#!/usr/bin/env python3

import argparse
import random
import sys

from dwq import Job, Disque
from dwq.version import __version__

def parse_args():
    parser = argparse.ArgumentParser(prog='dwqm', description='dwq: disque-based work queue (management tool)')

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_queue = subparsers.add_parser('queue', help='queue help')
    parser_queue.set_defaults(func=queue)

    group = parser_queue.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--show', help='show disque queue(s)', action='store_true')
    group.add_argument('-d', '--drain', help='empty disque queue(s)', action='store_true')
    parser_queue.add_argument('queues', type=str, nargs='*')

    parser_control = subparsers.add_parser('control', help='control help')
    parser_control.set_defaults(func=control)
    group = parser_control.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--list', help='list node(s)', action='store_true')
    group.add_argument('-p', '--pause', help='pause node(s)', action='store_true')
    group.add_argument('-r', '--resume', help='resume node(s)', action='store_true')
    group.add_argument('-s', '--shutdown', help='shutdown node(s)', action='store_true')
    parser_control.add_argument('nodes', type=str, nargs='*')

    parser.add_argument('--version', action='version', version='%(prog)s ' + __version__)

    return parser.parse_args()

def print_queue(name, qstat):
    print("name:", name, "len:", qstat['len'], "blocked:", qstat['blocked'])

def show(queues):
    Disque.connect(["localhost:7711"])

    qstat = Disque.qstat(queues)
    queues = sorted(qstat.keys())

    for name in queues:
        try:
            queue = qstat[name]
            print_queue(name, queue)
        except KeyError:
            print("invalid queue \"%s\"" % name)

def drain(queues):
    if not queues:
        print("dwqm: drain: no queues given.")
        sys.exit(1)

    Disque.connect(["localhost:7711"])
    disque = Disque.get()
    try:
        while True:
            jobs = Job.get(queues, count=1024, nohang=True)
            if not jobs:
                return

            job_ids = []
            for job in jobs:
                job_ids.append(job.job_id)

            disque.fast_ack(*job_ids)
    except KeyboardInterrupt:
        pass

def queue(args):
    if args.drain:
        drain(args.queues)
    elif args.show:
        show(args.queues)

def control(args):
    Disque.connect(["localhost:7711"])

    if args.pause:
        control_cmd(args.nodes, "pause")
    elif args.shutdown:
        control_cmd(args.nodes, "shutdown")
    elif args.resume:
        control_cmd(args.nodes, "resume")

def control_cmd(nodes, cmd):
    control_queue = "control::%s" % str(random.random())
    job_ids = []
    for node in nodes:
        print("dwqm: sending \"%s\" command to node \"%s\"" % (cmd, node))
        job_id = control_send_cmd(node, cmd, control_queue)
        job_ids.append(job_id)

def control_send_cmd(worker_name, cmd, control_queue):
    body = { "control" : { "cmd" : cmd }}
    job_id = Job.add("control::worker::%s" % worker_name, body, [control_queue])
    return job_id

def main():
    args = parse_args()
    if "func" in args:
        args.func(args)
    else:
        print("dwqm: no command given")
        sys.exit(1)
