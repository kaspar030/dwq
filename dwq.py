import json
from pydisque.client import Client

disque = None

class Disque(object):
    def connect(servers):
        global disque
        disque = Client(servers)
        disque.connect()

    def get():
        global disque
        return disque

    def connected():
        global disque
        if not disque:
            return False
        if not disque.connected_node:
            return False
        return True

class Job(object):
    def __init__(s, job_id, body, queue_name, nacks, additional_deliveries):
        s.job_id = job_id
        s.body = body
        s.queue_name = queue_name
        s.nacks = nacks
        s.additional_deliveries = additional_deliveries
        s.status_queues = body.get("status_queues") or []

    def get(queues, timeout=None, count=None, nohang=False):
        global disque
        jobs = []
        _jobs = disque.get_job(queues, timeout=timeout, count=count, nohang=nohang, withcounters=True)
        for queue_name, job_id, json_body, nacks, additional_deliveries in _jobs:
            queue_name = queue_name.decode("ascii")
            job_id = job_id.decode("ascii")
            body = json.loads(json_body.decode("utf-8"))
            jobs.append(Job(job_id, body, queue_name, nacks, additional_deliveries))

        return jobs

    def working(s, status):
        global disque
        disque.working(s.job_id)

    def done(s, result):
        for queue in s.status_queues:
            if queue=="$jobid":
                queue = s.job_id
            disque.add_job(queue, json.dumps({"job_id" : s.job_id, "state" : "done", "result" : result }))

        disque.ack_job(s.job_id)

    def add(queue, body, status_queues=None, **kwargs):
        body["status_queues"] = status_queues or []

        json_body = json.dumps(body)
        return disque.add_job(queue, json_body, **kwargs).decode("ascii")

    def nack(s):
        disque.nack_job(s.job_id)

    def cancel(s):
        disque.del_job(s.job_id)

    def wait(queue, count=None):
        _jobs = disque.get_job([queue], count=count)
        jobs = []
        for queue_name, job_id, json_body in _jobs:
            queue_name = queue_name.decode("ascii")
            job_id = job_id.decode("ascii")
            body = json.loads(json_body.decode("utf-8"))
            disque.fast_ack(job_id)
            jobs.append(body)
        return jobs

    def cancel_all(job_ids):
        disque.del_job(*tuple(job_ids))
