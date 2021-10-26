import os, json

class Config(object):
    EXTENSIONS = ['csv', 'xls', 'xlsx', 'json']
    def __init__(self, args) -> None:
        self.FILES_DIR = os.path.abspath(args.files)
        self.RECURSIVE = args.recursive
        self.EXTENSION = args.extension
        self.INDEX = args.index
        self.PROCESSES = args.processes

        if not self.FILES_DIR.endswith('/') or not self.FILES_DIR.endswith('\\'):
            self.FILES_DIR += '/'

    def __str__(self) -> str:
        return json.dumps({
            "FILES_DIR": self.FILES_DIR,
            "RECURSIVE": self.RECURSIVE,
            "EXTENSION": self.EXTENSION,
            "INDEX": self.INDEX,
            "PROCESSES": self.PROCESSES,
        }, indent=4)