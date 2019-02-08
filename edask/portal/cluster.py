import os, time
from edask.util.logging import EDASLogger
from distributed.deploy.ssh import start_worker
from .scheduler import getHost
from threading import Thread
from edask.config import EdaskEnv
from tornado import gen
import click

@click.command(help="""Launch a distributed cluster over SSH. A 'dask-scheduler' process will run on the
                         first host specified in [HOSTNAMES] or in the hostfile (unless --scheduler is specified
                         explicitly). One or more 'dask-worker' processes will be run each host in [HOSTNAMES] or
                         in the hostfile. Use command line flags to adjust how many dask-worker process are run on
                         each host (--nprocs) and how many cpus are used by each dask-worker process (--nthreads).""")
@click.option('--scheduler', default=None, type=str,
              help="Specify scheduler node.  Defaults to first address.")
@click.option('--scheduler-port', default=8786, type=int,
              help="Specify scheduler port number.  Defaults to port 8786.")
@click.option('--nthreads', default=0, type=int,
              help=("Number of threads per worker process. "
                    "Defaults to number of cores divided by the number of "
                    "processes per host."))
@click.option('--nprocs', default=1, type=int,
              help="Number of worker processes per host.  Defaults to one.")
@click.argument('hostnames', nargs=-1, type=str)
@click.option('--hostfile', default=None, type=click.Path(exists=True),
              help="Textfile with hostnames/IP addresses")
@click.option('--ssh-username', default=None, type=str,
              help="Username to use when establishing SSH connections.")
@click.option('--ssh-port', default=22, type=int,
              help="Port to use for SSH connections.")
@click.option('--ssh-private-key', default=None, type=str,
              help="Private key file to use for SSH connections.")
@click.option('--nohost', is_flag=True,
              help="Do not pass the hostname to the worker.")
@click.option('--log-directory', default=None, type=click.Path(exists=True),
              help=("Directory to use on all cluster nodes for the output of "
                    "dask-scheduler and dask-worker commands."))
@click.option('--remote-python', default=None, type=str,
              help="Path to Python on remote nodes.")
@click.option('--memory-limit', default='auto',
              help="Bytes of memory that the worker can use. "
                   "This can be an integer (bytes), "
                   "float (fraction of total system memory), "
                   "string (like 5GB or 5000M), "
                   "'auto', or zero for no memory management")
@click.option('--worker-port', type=int, default=0,
              help="Serving computation port, defaults to random")
@click.option('--nanny-port', type=int, default=0,
              help="Serving nanny port, defaults to random")
@click.pass_context

def main(ctx, scheduler, scheduler_port, hostnames, hostfile, nthreads, nprocs,
         ssh_username, ssh_port, ssh_private_key, nohost, log_directory, remote_python,
         memory_limit, worker_port, nanny_port):
    try:
        hostnames = list(hostnames)
        if hostfile:
            with open(hostfile) as f:
                hosts = f.read().split()
            hostnames.extend(hosts)

        if not scheduler:
            scheduler = hostnames[0]

    except IndexError:
        print(ctx.get_help())
        exit(1)

    c = EDASCluster(scheduler, scheduler_port, hostnames, nthreads, nprocs,
                   ssh_username, ssh_port, ssh_private_key, nohost, log_directory, remote_python,
                   memory_limit, worker_port, nanny_port)

    import distributed
    print('\n---------------------------------------------------------------')
    print('                 Dask.distributed v{version}\n'.format(version=distributed.__version__))
    print('Worker nodes:'.format(n=len(hostnames)))
    for i, host in enumerate(hostnames):
        print('  {num}: {host}'.format(num=i, host=host))
    print('\nscheduler node: {addr}:{port}'.format(addr=scheduler, port=scheduler_port))
    print('---------------------------------------------------------------\n\n')

    # Monitor the output of remote processes.  This blocks until the user issues a KeyboardInterrupt.
    c.monitor_remote_processes()

    # Close down the remote processes and exit.
    print("\n[ dask-ssh ]: Shutting down remote processes (this may take a moment).")
    c.shutdown()
    print("[ dask-ssh ]: Remote processes have been terminated. Exiting.")


def go():
    main()

class EDASCluster(object):

    def __init__(self, scheduler_addr, scheduler_port, worker_addrs, nthreads=0, nprocs=1,
                 ssh_username=None, ssh_port=22, ssh_private_key=None,
                 nohost=False, logdir=None, remote_python=None,
                 memory_limit=None, worker_port=None, nanny_port=None):

        self.scheduler_addr = scheduler_addr
        self.scheduler_port = scheduler_port
        self.nthreads = nthreads
        self.nprocs = nprocs

        self.ssh_username = ssh_username
        self.ssh_port = ssh_port
        self.ssh_private_key = ssh_private_key
        self.logdir = logdir if logdir else os.path.expanduser( "~/.edask/logs" )
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


class EDASKClusterThread(Thread):

    def __init__(self, nthreads=0, nprocs=1, ssh_username=None, ssh_port=22, ssh_private_key=None, nohost=False, remote_python=None, memory_limit=None, worker_port=None, nanny_port=None ):
        Thread.__init__(self)
        self.logger = EDASLogger.getLogger()
        self.nthreads = nthreads
        self.nprocs = nprocs
        self.worker_addrs = self.getHosts()

        self.ssh_username = ssh_username
        self.ssh_port = ssh_port
        self.ssh_private_key = ssh_private_key
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

if __name__ == '__main__':
    go()