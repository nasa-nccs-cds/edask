import os, time
from tornado import gen
from edask.util.logging import EDASLogger
from distributed.deploy.ssh import start_worker
from .scheduler import getHost


class EDASKCluster(object):

    def __init__(self, worker_addrs, scheduler_port=8786, nthreads=0, nprocs=1, ssh_username=None, ssh_port=22, ssh_private_key=None, nohost=False, remote_python=None, memory_limit=None, worker_port=None, nanny_port=None ):
        self.logger = EDASLogger.getLogger()
        self.nthreads = nthreads
        self.nprocs = nprocs

        self.ssh_username = ssh_username
        self.ssh_port = ssh_port
        self.ssh_private_key = ssh_private_key
        self.scheduler_addr = getHost()
        self.scheduler_port = scheduler_port
        self.logdir = os.path.expanduser( "~/.edask/logs" )

        self.nohost = nohost

        self.remote_python = remote_python

        self.memory_limit = memory_limit
        self.worker_port = worker_port
        self.nanny_port = nanny_port

        # Keep track of all running threads
        self.threads = []

        # Start worker nodes
        self.workers = []
        for i, addr in enumerate(worker_addrs):
            self.add_worker(addr)

    @gen.coroutine
    def _start(self):
        pass

    @property
    def scheduler_address(self):
        return '%s:%d' % (self.scheduler_addr, self.scheduler_port)

    def monitor_remote_processes(self):

        # Form a list containing all processes, since we treat them equally from here on out.
        all_processes = self.workers

        try:
            while True:
                for process in all_processes:
                    while not process['output_queue'].empty():
                        print(process['output_queue'].get())

                # Kill some time and free up CPU before starting the next sweep
                # through the processes.
                time.sleep(0.1)

            # end while true

        except KeyboardInterrupt:
            pass   # Return execution to the calling process

    def add_worker(self, address):
        self.workers.append(start_worker(self.logdir, self.scheduler_addr,
                                         self.scheduler_port, address,
                                         self.nthreads, self.nprocs,
                                         self.ssh_username, self.ssh_port,
                                         self.ssh_private_key, self.nohost,
                                         self.memory_limit,
                                         self.worker_port,
                                         self.nanny_port,
                                         self.remote_python))

    def shutdown(self):
        all_processes = self.workers

        for process in all_processes:
            process['input_queue'].put('shutdown')
            process['thread'].join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()
