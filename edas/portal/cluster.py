import os, time, socket, sys, json
from edas.util.logging import EDASLogger
from typing import Sequence, List, Dict, Mapping, Optional
from distributed.deploy.ssh import start_worker
from threading import Thread
import subprocess
from edas.config import EdasEnv
from distributed.deploy import Cluster

def get_private_key():
    pkey_opts = os.environ.get('PKEY_OPTS', None)
    if pkey_opts is not None:
        for toks in [opts.split("=") for opts in pkey_opts.split(" ")]:
            if "ssh-private-key" in toks[0]:
                return toks[1]
    user = os.environ.get('USER', "")
    for pkey in  [ os.path.expanduser("~/.ssh/id_" + user), os.path.expanduser("~/.ssh/id_rsa") ]:
        if os.path.isfile(pkey):  return pkey
    return None

def getHost():
    return [l for l in (
    [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [
        [(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
         [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

class EDASKClusterThread(Thread):

    def __init__(self, nthreads=0, nprocs=1, nohost=False, remote_python=None, memory_limit=None, worker_port=None, nanny_port=None ):
        Thread.__init__(self)
        self.logger = EDASLogger.getLogger()
        self.nthreads = nthreads
        self.nprocs = nprocs
        self.worker_addrs = self.getHosts()

        self.ssh_username = os.environ.get( 'USER', None )
        self.ssh_port = 22
        self.ssh_private_key = get_private_key()
        self.scheduler_addr = getHost()
        self.scheduler_port = int(EdasEnv.get("scheduler.port", 8786))
        self.logdir = os.path.expanduser( "~/.edas/logs" )
        self.active = False

        self.nohost = nohost
        self.remote_python = remote_python
        self.memory_limit = memory_limit
        self.worker_port = worker_port
        self.nanny_port = nanny_port

        # Keep track of all running threads
        self.threads = []

    def getHosts( self ):
        hostfile = EdasEnv.get("hostfile.path", os.path.expanduser("~/.edas/conf/hosts"))
        assert os.path.isfile( hostfile ), "Error, the EDAS hosts file '{}' does not exist.  Copy edas/resourses/hosts.template to '{}' and edit.".format( hostfile, hostfile )
        with open(hostfile) as f:
           return f.read().split()

    def run(self):
        self.workers = []
        for i, addr in enumerate(self.worker_addrs):
            self.add_worker(addr)
        self.monitor_remote_processes()

    @property
    def scheduler_address(self):
        return '%s:%d' % (self.scheduler_addr, self.scheduler_port)

    def monitor_remote_processes(self):
        self.active = True
        all_processes = self.workers
        try:
            while self.active:
                for process in all_processes:
                    while not process['output_queue'].empty():
                        self.logger.info( "@@WM: " + (process['output_queue'].get()) )
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    def add_worker(self, address):
        self.workers.append( start_worker(self.logdir, self.scheduler_addr,
                                         self.scheduler_port, address,
                                         self.nthreads, self.nprocs,
                                         self.ssh_username, self.ssh_port,
                                         self.ssh_private_key, self.nohost,
                                         self.memory_limit,
                                         self.worker_port,
                                         self.nanny_port,
                                         self.remote_python))

    def scale_up(self, n: int ):
         pass

    def scale_down( self, workers ):
        pass

    def shutdown(self):
        self.active = False
        all_processes = self.workers
        for process in all_processes:
            process['input_queue'].put('shutdown')
            process['thread'].join()
            self.workers.remove(process)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

class EDASCluster(Cluster):
    EDASK_BIN_DIR = os.path.dirname( sys.executable )
    SCHEDULER_SCRIPT =  os.path.join( EDASK_BIN_DIR, 'startup_scheduler' )

    def __init__(self):
        Cluster.__init__(self)
        self.logger = EDASLogger.getLogger()
        self.scheduler_host = getHost()
        self.scheduler_port =  int(EdasEnv.get("scheduler.port", 8786))
        self.schedulerProcess = self.startup_scheduler( )
        time.sleep(14)
        self.clusterThread = self.startup_cluster()

    @property
    def scheduler_address(self) -> str:
        return self.scheduler_host + ":" + str( self.scheduler_port )

    def scale_up(self, n: int ):
         if self.clusterThread is not None: self.clusterThread.scale_up(n)

    def scale_down( self, workers ):
         if self.clusterThread is not None: self.clusterThread.scale_down( workers )

    def shutdown(self):
        if self.schedulerProcess is not None: self.schedulerProcess.terminate()
        if self.clusterThread is not None: self.clusterThread.shutdown()

    def startup_scheduler( self  ):
        if not EdasEnv.getBool("edas.manage.scheduler"): return None
#        os.environ["PKEY_OPTS"]  = "--ssh-private-key=" + get_private_key()
        os.environ["PATH"] = ":".join( [ self.EDASK_BIN_DIR, os.environ["PATH"] ] )
        bokeh_port = int(EdasEnv.get("dashboard.port", 8787))
        args = [ sys.executable, self.SCHEDULER_SCRIPT, "--host", self.scheduler_host, "--port", str(self.scheduler_port), "--bokeh-port", str(bokeh_port) ]
        return subprocess.Popen( args )

    def startup_cluster( self ):
        if not EdasEnv.getBool("edas.manage.cluster"): return None
        clusterThread = EDASKClusterThread()
        clusterThread.start()
        return clusterThread

    def schedulerRequest( self, **kwargs ) -> Dict:
        if self.schedulerProcess is None: return dict( error="Can't communicate with the scheduler because it is not managed by edas")
        self.schedulerProcess.stdin.writelines( [ json.dumps(kwargs) ] )
        return json.loads( self.schedulerProcess.stdout.readline() )

    def getMetrics( self, mtype: str ) -> Dict:
        return self.schedulerRequest( op="metrics", type=mtype )



