# dwq - a Disque based work queue

dwq (Disque Work Queue) is a tool that can be used to distribute Jobs on git
repositories across multiple machines.

## Installation

    # pip install dwq

## Usage

Start a worker:

    # dwqw

Execute a command on worker:

    # dwqc -r <repository> -c <commit> ls

More sophisticated:

    # export DWQ_REPO=<repository> DWQ_COMMIT=<commit>
    # for i in $(seq 10); do echo 'echo $DWQ_WORKER:$DWQ_WORKER_THREAD'; done | dwqc
