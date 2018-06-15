import json
from subprocess import Popen, PIPE
from itertools import imap

class DumpReader():

    def __init__(self,fname, filter_cmd=""):
        self.fname = fname
	self.proc = None
        self.filter_cmd = filter_cmd

    def __enter__(self):

        filter = ""
        if self.filter_cmd:
            filter = """ | {} """.format(self.filter_cmd)

        if self.fname.endswith(".bz2"):
            self.proc = Popen("bzcat {} {} ".format(self.fname, filter), shell=True, stdout=PIPE, universal_newlines=True)
        else:
            self.proc = Popen("cat {} {} ".format(self.fname, filter), shell=True, stdout=PIPE, universal_newlines=True)

        return imap(json.loads, self.proc.stdout) 

    def __exit__(self, type, value, traceback): 
        if self.proc is not None: 
            self.proc.kill()
        return False
