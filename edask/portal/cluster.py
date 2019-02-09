import os, time
from edask.util.logging import EDASLogger
from distributed.deploy.ssh import start_worker
from .scheduler import getHost
from threading import Thread
from edask.config import EdaskEnv
from distributed.deploy import Cluster
from edask.portal.scheduler import SchedulerThread
from distributed import Scheduler

def get_private_key():
    pkey_opts = os.environ.get('PKEY_OPTS', None)
    if pkey_opts is not None:
        for toks in [opts.split("=") for opts in pkey_opts.split(" ")]:
            if "ssh-private-key" in toks[0]:
                return toks[1]
    user = os.environ.get('USER', "")
    return os.path.expanduser("~/.ssh/id_" + user)

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
        self.scheduler_port = int( EdaskEnv.get("scheduler.port", 8786 ) )
        self.logdir = os.path.expanduser( "~/.edask/logs" )
        self.active = False

        self.nohost = nohost
        self.remote_python = remote_python
        self.memory_limit = memory_limit
        self.worker_port = worker_port
        self.nanny_port = nanny_port

        # Keep track of all running threads
        self.threads = []

    def getHosts( self ):
        hostfile = EdaskEnv.get("hostfile.path", os.path.expanduser( "~/.edask/conf/hosts") )
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

    def __init__(self):
        Cluster.__init__(self)
        self.schedulerThread = SchedulerThread()
        self.schedulerThread.start()
        self.clusterThread = EDASKClusterThread()
        self.clusterThread.start()

    @property
    def scheduler(self) -> Scheduler:
        return self.schedulerThread._scheduler

    def scale_up(self, n: int ):
         self.clusterThread.scale_up(n)

    def scale_down( self, workers ):
         self.clusterThread.scale_down( workers )

    def shutdown(self):
        self.schedulerThread.shutdown()
        self.clusterThread.shutdown()



