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
        parts = name.split(":", maxsplit=1)
        if len(parts) == 2:
            local, remote = parts
            if remote.startswith("/"):
                raise GenFileDataException(
                    "file %s: remote name %s must be relative" % (local, remote)
                )

            if ".." in remote:
                raise GenFileDataException(
                    'file %s: no ".." allowed in remote name %s' % (local, remote)
                )
        else:
            local = name
            if name.startswith("/"):
                if not name.startswith(root + "/"):
                    raise Exception(
                        "file %s: remote path must be relative to cwd" % local
                    )
                else:
                    remote = os.path.abspath(name)[len(root) + 1 :]
            else:
                remote = name

        if not local.startswith("/"):
            local = os.path.join(root, local)
        try:
            res[remote] = base64_file(local)
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
