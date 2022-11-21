#!/usr/bin/env python3

import json
import random
import os
import signal
import sys
import time
import argparse

from dwq import Job, Disque
import dwq.util as util

from dwq.version import __version__


def sigterm_handler(signal, stack_frame):
    raise SystemExit()


def nicetime(time):
    secs = round(time)
    minutes = secs / 60
    hrs = minutes / 60
    days = int(hrs / 24)
    secs = int(secs % 60)
    minutes = int(minutes % 60)
    hrs = int(hrs % 24)
    res = ""
    if days:
        res += "%id:" % days
    if hrs:
        res += "%ih:" % hrs
    if minutes:
        if hrs and minutes < 10:
            res += "0"
        res += "%im:" % minutes
    if minutes and secs < 10:
        res += "0"
    res += "%is" % secs
    return res


def parse_args():
    parser = argparse.ArgumentParser(
        prog="dwqc", description="dwq: disque-based work queue"
    )

    parser.add_argument(
        "-q",
        "--queue",
        type=str,
        help='queue name for jobs (default: "default")',
        default=os.environ.get("DWQ_QUEUE") or "default",
    )

    parser.add_argument(
        "-r",
        "--repo",
        help="git repository to work on",
        type=str,
        default=os.environ.get("DWQ_REPO"),
    )

    parser.add_argument(
        "-c",
        "--commit",
        help="git commit to work on",
        type=str,
        default=os.environ.get("DWQ_COMMIT"),
    )

    parser.add_argument(
        "-e",
        "--exclusive-jobdir",
        help="don't share jobdirs between jobs",
        action="store_true",
    )

    parser.add_argument(
        "-P", "--progress", help="enable progress output", action="store_true"
    )
    parser.add_argument(
        "-R", "--report", help="report to disque queue", action="store", type=str
    )
    parser.add_argument(
        "-v", "--verbose", help="enable status output", action="store_true"
    )
    parser.add_argument(
        "-Q", "--quiet", help="don't print command output", action="store_true"
    )
    parser.add_argument(
        "-m",
        "--maxfail",
        help="exit after more than <maxfail> jobs failed",
        type=int,
        default=sys.maxsize,
    )
    parser.add_argument(
        "-n",
        "--no-error-output",
        help="don't print output of errored jobs",
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--tries",
        help="try job at most n times (n>0, default: 3)",
        type=int,
        default=3,
    )
    parser.add_argument(
        "-T",
        "--timeout",
        help="job timeout (max runtime *after* queue time) (n>0, default: 300)",
        type=int,
        default=300,
    )
    parser.add_argument("-s", "--stdin", help="read from stdin", action="store_true")
    parser.add_argument(
        "-o", "--outfile", help="write job results to file", type=argparse.FileType("w")
    )
    parser.add_argument(
        "-b", "--batch", help="send all jobs together", action="store_true"
    )
    parser.add_argument(
        "-S",
        "--subjob",
        help="pass job(s) to master instance, don't wait for completion",
        action="store_true",
    )
    parser.add_argument(
        "-E",
        "--env",
        help="export environment variable to client",
        type=str,
        action="append",
        default=[],
    )
    parser.add_argument(
        "-F",
        "--file",
        help="send file along with job",
        type=str,
        action="append",
        default=[],
    )
    parser.add_argument(
        "-A", "--asset-dir", help="save all assets to specified folder", type=str, action="store"
    )

    parser.add_argument(
        "-D", "--disque-url", help="specify disque instance [default: localhost:7711]",
        type=str, action="store", default=os.environ.get("DWQ_DISQUE_URL", "localhost:7711"),
    )

    parser.add_argument(
        "--version", action="version", version="%(prog)s " + __version__
    )

    parser.add_argument("command", type=str, nargs="?")

    return parser.parse_args()


def get_env(env):
    result = {}
    for var in env:
        var = var.split("=", maxsplit=1)
        if len(var) == 1:
            val = os.environ.get(var[0])
            if val:
                var.append(val)
            else:
                continue
        result[var[0]] = var[1]

    return result


def create_body(args, command, options=None, parent_id=None):
    body = {"command": command}

    if args.repo:
        body["repo"] = args.repo

    if args.commit:
        body["commit"] = args.commit

    if options:
        body["options"] = options

    if parent_id:
        body["parent"] = parent_id

    env = {}
    if args.env:
        env.update(get_env(args.env))

    env.update(options.get("env", {}))
    if env:
        body["env"] = env

    return body


def queue_job(jobs_set, queue, body, control_queues):
    timeout = body.get("options", {}).get("timeout")
    if timeout:
        job_id = Job.add(queue, body, control_queues, retry=timeout)
    else:
        job_id = Job.add(queue, body, control_queues)

    parent = body.get("parent")
    if parent:
        body = {
            "parent": parent,
            "subjob": job_id,
            "unique": os.environ.get("DWQ_JOB_UNIQUE"),
        }
        Job.add(control_queues[0], body, None)

    else:
        jobs_set.add(job_id)

    return job_id


def vprint(*args, **kwargs):
    global verbose
    if verbose:
        print(*args, **kwargs)


verbose = False


def dict_addset(_dict, key, data):
    try:
        _dict[key].add(data)
    except KeyError:
        _dict[key] = {data}


def dict_dictadd(_dict, key):
    try:
        return _dict[key]
    except KeyError:
        _tmp = {}
        _dict[key] = _tmp
        return _tmp


def handle_assets(job, args):
    assets = job["result"].get("assets")

    if assets:
        if args.asset_dir is None:
            print('dwqc: warning: assets ignored (no asset-dir specified)')
            return

        asset_dir = os.path.abspath(args.asset_dir)
        for remote, data in assets.items():
            if asset_dir:
                local = os.path.abspath(os.path.join(asset_dir, remote))
                if not local.startswith(asset_dir):
                    print(
                        'dwqc: warning: asset "%s" is not relative'
                        ' to "%s", ignoring' % (remote, asset_dir), local,
                        file=sys.stderr,
                    )
                    continue
            else:
                print(
                    'dwqc: warning: ignoring asset "%s"' % remote, file=sys.stderr
                )
                continue

            util.write_files({local: data})


def main():
    global verbose
    args = parse_args()

    if (args.repo is None) ^ (args.commit is None):
        print("dwqc: error: both repo and commit must be specified!", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGTERM, sigterm_handler)

    job_queue = args.queue

    Disque.connect([args.disque_url])

    if args.subjob:
        try:
            control_queue = os.environ["DWQ_CONTROL_QUEUE"]
        except KeyError:
            print("dwqc: error: --subjob specified, but DWQ_CONTROL_QUEUE unset.")
            sys.exit(1)

        try:
            parent_jobid = os.environ["DWQ_JOBID"]
        except KeyError:
            print("dwqc: error: --subjob specified, but DWQ_JOBID unset.")
            sys.exit(1)

    else:
        control_queue = "control::%s" % random.random()
        parent_jobid = None

    verbose = args.verbose

    if args.progress or args.report:
        start_time = time.time()

    if args.report:
        Job.add(args.report, {"status": "collecting jobs"})

    try:
        file_data = util.gen_file_data(args.file)
    except util.GenFileDataException as e:
        print("dwqc: error processing --file argument:", e, file=sys.stderr)
        sys.exit(1)

    base_options = {}
    if args.exclusive_jobdir:
        base_options.update({"jobdir": "exclusive"})
    if file_data:
        base_options["files"] = file_data
    if args.tries != 3:
        if args.tries < 1:
            print("dwqc: error: --tries < 1!")
            sys.exit(1)

        base_options["max_retries"] = args.tries - 1

    if args.timeout != 300:
        if args.timeout <= 0:
            print("dwqc: error: --timeout <=0!")
            sys.exit(1)

        base_options["timeout"] = args.timeout

    result_list = []
    try:
        jobs = set()
        batch = []
        if args.command and not args.stdin:
            options = base_options
            queue_job(
                jobs,
                job_queue,
                create_body(args, args.command, options, parent_jobid),
                [control_queue],
            )
        else:
            jobs_read = 0
            vprint("dwqc: reading jobs from stdin")
            for line in sys.stdin:
                line = line.rstrip()
                if args.stdin and args.command:
                    cmdargs = line.split(" ")
                    command = args.command
                    for i in range(0, len(cmdargs)):
                        command = command.replace("${%i}" % (i + 1), cmdargs[i])
                    command = command.replace("${0}", line)
                else:
                    command = line

                tmp = command.split("###")
                command = tmp[0]
                options = {}
                options.update(base_options)
                if len(tmp) > 1:
                    command = command.rstrip()
                    try:
                        options.update(json.loads(tmp[1]))
                    except json.decoder.JSONDecodeError:
                        vprint(
                            "dwqc: invalid option JSON. Skipping job.", file=sys.stderr
                        )
                        continue

                _job_queue = options.get("queue", job_queue)

                if args.batch:
                    batch.append(
                        (
                            _job_queue,
                            create_body(args, command, options, parent_jobid),
                            [control_queue],
                        )
                    )
                else:
                    job_id = queue_job(
                        jobs,
                        _job_queue,
                        create_body(args, command, options, parent_jobid),
                        [control_queue],
                    )
                    vprint(
                        'dwqc: job %s command="%s" sent to queue %s.'
                        % (job_id, command, _job_queue)
                    )
                    if args.progress:
                        print("")

                if args.progress or args.report:
                    jobs_read += 1
                    elapsed = time.time() - start_time

                    if args.progress:
                        print(
                            "\033[F\033[K[%s] %s jobs read"
                            % (nicetime(elapsed), jobs_read),
                            end="\r",
                        )

                    # if args.report:
                    #    Job.add(args.report, { "status" : "collecting jobs", "total" : jobs_read })

        _time = ""
        if args.batch and args.stdin:
            before = time.time()
            vprint("dwqc: sending jobs")
            for _tuple in batch:
                queue_job(jobs, *_tuple)
            _time = "(took %s)" % nicetime(time.time() - before)

            if args.report:
                Job.add(args.report, {"status": "sending jobs"})

        if args.stdin:
            vprint("dwqc: all jobs sent.", _time)

        if args.subjob:
            if args.report:
                Job.add(args.report, {"status": "done"})
            return

        if args.progress:
            vprint("")

        unexpected = {}
        early_subjobs = []
        total = len(jobs)
        done = 0
        failed = 0
        failed_expected = 0
        passed = 0
        subjobs = {}
        while jobs:
            _early_subjobs = early_subjobs or None
            early_subjobs = []

            for job in _early_subjobs or Job.wait(control_queue, count=128):
                # print(json.dumps(job, sort_keys=True, indent=4))
                subjob = job.get("subjob")
                if subjob:
                    parent = job.get("parent")
                    unique = job.get("unique")

                    _dict = dict_dictadd(subjobs, parent)
                    dict_addset(_dict, unique, subjob)

                else:
                    try:
                        job_id = job["job_id"]
                        jobs.remove(job_id)
                        done += 1
                        _has_passed = job["result"]["status"] in {0, "0", "pass"}
                        # if args.progress:
                        #    vprint("\033[F\033[K", end="")
                        # vprint("dwqc: job %s done. result=%s" % (job["job_id"], job["result"]["status"]))
                        if not args.quiet:
                            if _has_passed or (not args.no_error_output):
                                if args.progress:
                                    print("\033[K", end="")
                                print(job["result"]["output"], end="")
                        if _has_passed:
                            passed += 1
                            handle_assets(job, args)
                        else:
                            failed += 1
                            try:
                                if job["result"]["body"]["options"]["fail_ok"]:
                                    failed_expected += 1
                            except KeyError:
                                pass

                        if args.outfile:
                            result_list.append(job)

                        # collect subjobs started by this job instance, add to waitlist
                        unique = job["result"]["unique"]
                        _subjobs = subjobs.get(job_id, {}).get(unique, [])
                        for subjob_id in _subjobs:
                            try:
                                early_subjobs.append(unexpected.pop(subjob_id))
                            except KeyError:
                                pass
                            finally:
                                jobs.add(subjob_id)

                        total += len(_subjobs)

                        if args.progress or args.report:
                            elapsed = time.time() - start_time
                            per_job = elapsed / done
                            eta = (total - done) * per_job

                            if args.progress:
                                print(
                                    "\r\033[K[%s] %s/%s jobs done (%s passed, %s failed.) "
                                    "ETA:"
                                    % (nicetime(elapsed), done, total, passed, failed),
                                    nicetime(eta),
                                    end="\r",
                                )

                            if args.report:
                                Job.add(
                                    args.report,
                                    {
                                        "status": "working",
                                        "elapsed": elapsed,
                                        "eta": eta,
                                        "total": total,
                                        "passed": passed,
                                        "failed": failed,
                                        "job": job,
                                    },
                                )

                        if not _has_passed:
                            if failed >= failed_expected:
                                if (failed - failed_expected) > args.maxfail:
                                    print(
                                        "dwqc: more than %i jobs failed. Exiting."
                                        % args.maxfail,
                                        file=sys.stderr,
                                    )
                                    sys.exit(1)
                    except KeyError:
                        unexpected[job_id] = job

        if args.outfile:
            args.outfile.write(json.dumps(result_list))

        if args.progress:
            print("")

    except (KeyboardInterrupt, SystemExit):
        print("dwqc: cancelling...")
        Job.cancel_all(jobs)
        if args.report:
            Job.add(args.report, {"status": "canceled"})

        if args.outfile:
            args.outfile.seek(0)
            args.outfile.write(json.dumps(result_list))

        sys.exit(1)

    if args.report:
        Job.add(args.report, {"status": "done"})

    if failed > failed_expected:
        sys.exit(1)
