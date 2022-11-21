import json
import base64
import os


class GenFileDataException(Exception):
    pass


def gen_file_data(names, root=None):
    res = {}
    if not root:
        root = os.getcwd()
    for name in names:
        local = name
        if not local.startswith("/"):
            local = os.path.join(root, local)
        try:
            res[name] = base64_file(local)
        except FileNotFoundError as e:
            raise GenFileDataException(e)

    return res


def base64_file(name):
    with open(name, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def write_files(data, workdir=None):
    data = data or {}
    for filename, filedata in data.items():
        if workdir:
            filename = os.path.join(workdir, filename)

        dirname = os.path.dirname(filename)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        with open(filename, "wb") as f:
            f.write(base64.b64decode(filedata))
