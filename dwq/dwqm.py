#!/usr/bin/env python3

import argparse
import random
import sys

from dwq import Job, Disque
from dwq.version import __version__


def parse_args():
    parser = argparse.ArgumentParser(
        prog="dwqm", description="dwq: disque-based work queue (management tool)"
    )

    subparsers = parser.add_subparsers(help="sub-command help")

    parser_queue = subparsers.add_parser("queue", help="queue help")
    parser_queue.set_defaults(func=queue)

    group = parser_queue.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-l", "--list", help="list all disque queue(s)", action="store_true"
    )
    group.add_argument(
        "-d", "--drain", help="empty disque queue(s)", action="store_true"
    )
    parser_queue.add_argument("queues", type=str, nargs="*")

    parser_control = subparsers.add_parser("control", help="control help")
    parser_control.set_defaults(func=control)
    group = parser_control.add_mutually_exclusive_group(required=True)
    group.add_argument("-l", "--list", help="list node(s)", action="store_true")
    group.add_argument("-p", "--pause", help="pause node(s)", action="store_true")
    group.add_argument("-r", "--resume", help="resume node(s)", action="store_true")
    group.add_argument("-s", "--shutdown", help="shutdown node(s)", action="store_true")
    group.add_argument("-P", "--ping", help="ping node(s)", action="store_true")

    #    parser_control.add_argument('-f', '--force', help='don\'t wait for job completion on pause/shutdown',
    #                                action='store_true')

    parser_control.add_argument("nodes", type=str, nargs="*")

    parser.add_argument(
        "--version", action="version", version="%(prog)s " + __version__
    )

    return parser.parse_args()


def print_queue(name, qstat):
    print("name:", name, "len:", qstat["len"], "blocked:", qstat["blocked"])


def listq(queues):
    Disque.connect(["localhost:7711"])

    qstat = Disque.qstat(queues)
    queues = sorted(qstat.keys())

    for name in queues:
        try:
            queue = qstat[name]
            print_queue(name, queue)
        except KeyError:
            print('invalid queue "%s"' % name)


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
    elif args.list:
        listq(args.queues)


def control(args):
    Disque.connect(["localhost:7711"])

    jobargs = {}
    #    if args.force:
    #        jobargs["force"] = True

    if args.pause:
        control_cmd(args.nodes, "pause", **jobargs)
    elif args.resume:
        control_cmd(args.nodes, "resume")
    elif args.shutdown:
        control_cmd(args.nodes, "shutdown", **jobargs)
    elif args.ping:
        control_cmd(args.nodes, "ping")
    elif args.list:
        list_nodes()


def get_nodes():
    queues = Disque.qscan()
    nodes = []

    for queue in queues:
        if queue.startswith("control::worker::"):
            nodes.append(queue[17:])

    return nodes


def list_nodes():
    nodes = get_nodes()
    for node in sorted(nodes):
        print(node)


def control_cmd(nodes, cmd, **kwargs):
    control_queue = "control::%s" % str(random.random())
    job_ids = []
    for node in nodes:
        print('dwqm: sending "%s" command to node "%s"' % (cmd, node))
        job_id = control_send_cmd(node, cmd, control_queue, **kwargs)
        job_ids.append(job_id)

    while job_ids:
        for job in Job.wait(control_queue, count=len(job_ids)):
            job_id = job["job_id"]
            job_ids.remove(job_id)
            try:
                print("%s:" % job["result"]["worker"], job["result"].get("output"))
            except KeyError:
                pass


def control_send_cmd(worker_name, cmd, control_queue, **kwargs):
    body = {"control": {"cmd": cmd}}
    if kwargs:
        body["control"]["args"] = kwargs

    job_id = Job.add("control::worker::%s" % worker_name, body, [control_queue])
    return job_id


def main():
    args = parse_args()
    if "func" in args:
        args.func(args)
    else:
        print("dwqm: no command given")
        sys.exit(1)
