from __future__ import print_function, division, absolute_import
import atexit, os, socket, shutil, sys, tempfile, time
from tornado.ioloop import IOLoop
from distributed import Scheduler
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.security import Security
from distributed.scheduler import ( TaskState, WorkerState )
from edask.util.logging import EDASLogger
from distributed.cli.utils import (install_signal_handlers, uri_from_host_port)
from distributed.proctitle import (enable_proctitle_on_children,enable_proctitle_on_current)
from threading import Thread
from edask.config import EdaskEnv

def getHost():
    return [l for l in (
    [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [
        [(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
         [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]


class EDASSchedulerPlugin(SchedulerPlugin):

     def __init__( self ):
         self.logger = EDASLogger.getLogger()
         self.scheduler: Scheduler = None

     def transition( self, key, start, finish, *args, **kwargs):
         self.logger.info( "@SP: transition[{}]: {} -> {}".format( key, start, finish ))
         if self.scheduler: SchedulerThread.log_metrics( self.logger, self.scheduler )

     def restart(self, scheduler: Scheduler, **kwargs ):
         self.logger.info("@SP: restart " )
         self.scheduler = scheduler

     def update_graph(self, scheduler: Scheduler, dsk=None, keys=None, restrictions=None, **kwargs):
        self.logger.info("@SP: update_graph ")
        self.scheduler = scheduler
        SchedulerThread.log_metrics( self.logger, self.scheduler )

class SchedulerThread(Thread):

    def __init__(self, **kwargs ):
        Thread.__init__(self)
        self.logger = EDASLogger.getLogger()
        self.host = getHost()
        self.port = int( EdaskEnv.get("scheduler.port", 8786 ) )
        self.bokeh_port = int( EdaskEnv.get("dashboard.port", 8787 ) )
        self.launch_dashboard = kwargs.get( "launch_dashboard", True )
        self.bokeh_whitelist = []
        self.bokeh_prefix  = kwargs.get( "bokeh_prefix", '' )
        self.pid_file = kwargs.get( "pid_file", '' )
        self.scheduler_file = kwargs.get( "scheduler_file", '' )
        self.local_directory = kwargs.get( "local_directory", '' )
        self.tls_ca_file = None
        self.tls_cert = None
        self.tls_key = None
        self.plugins = [ EDASSchedulerPlugin() ]
        self._scheduler: Scheduler = None
        self.local_directory_created = False
        self.loop = None

    @staticmethod
    def log_metrics( logger, scheduler ):
        if scheduler is not None:
            logger.info( "SCHEDULER METRICS:")
            logger.info( " * total_ncores: {}".format( str(scheduler.total_ncores) ) )
            logger.info( " * total_occupancy: {}".format( str(scheduler.total_occupancy) ))
            for (tkey,task) in scheduler.tasks.items():
                worker_name = task.processing_on.name if task.processing_on is not None else "None"
                logger.info(" --- TASK[{}]: state={}, processing_on={}".format( tkey, task.state, worker_name ))
            for (wkey,worker) in scheduler.workers.items():
                if len(worker.processing.items()) > 0:
                    processing = "; ".join( [ task.key + ": " + str(cost) for (task,cost) in worker.processing.items() ] )
                    logger.info(" ------ WORKER[{}:{}]({}): ncores={}, nbytes={}, processing= {}, metrics={}".format( wkey, worker.name, worker.address, worker.ncores, worker.nbytes, processing, str(worker.metrics) ))

    def shutdown(self):
        if self._scheduler is not None:
            self.loop.stop()
            self._scheduler.stop()
            if self.local_directory_created:
                shutil.rmtree(self.local_directory)
            self.logger.info("End scheduler at %r", str(self._scheduler.address))
            self._scheduler = None

    @property
    def scheduler(self) -> Scheduler:
        for iTry in range(100):
            if self._scheduler is not None: break
            time.sleep(0.2)
        return self._scheduler

    def run(self):
        enable_proctitle_on_current()
        enable_proctitle_on_children()
        sec = Security(tls_ca_file=self.tls_ca_file, tls_scheduler_cert=self.tls_cert, tls_scheduler_key=self.tls_key )

        if not self.host and (self.tls_ca_file or self.tls_cert or self.tls_key):
            self.host = 'tls://'

        if self.pid_file:
            with open(self.pid_file, 'w') as f:
                f.write(str(os.getpid()))

            def del_pid_file():
                if os.path.exists(self.pid_file):
                    os.remove(self.pid_file)
            atexit.register(del_pid_file)

        if self.local_directory:
            if not os.path.exists(self.local_directory):
                os.mkdir(self.local_directory)
                self.local_directory_created = True
        else:
            self.local_directory = tempfile.mkdtemp(prefix='scheduler-')
            self.local_directory_created = True
        if self.local_directory not in sys.path:
            sys.path.insert(0, self.local_directory)

        if sys.platform.startswith('linux'):
            import resource   # module fails importing on Windows
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            limit = max(soft, hard // 2)
            resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

        addr = uri_from_host_port(self.host, self.port, 8786)
        self.loop = IOLoop.current()
        self.logger.info('-' * 47)

        services = {}
        if self.launch_dashboard:
            try:
                from distributed.bokeh.scheduler import BokehScheduler
                services[('bokeh', self.bokeh_port)] = (BokehScheduler, {'prefix': self.bokeh_prefix})
            except ImportError as error:
                if str(error).startswith('No module named'):
                    self.logger.info('Web dashboard not loaded.  Unable to import bokeh')
                else:
                    self.logger.info('Unable to import bokeh: %s' % str(error))

        self._scheduler = Scheduler(loop=self.loop, services=services, scheduler_file=self.scheduler_file, security=sec)
        self._scheduler.add_plugin(self.plugin)
        self._scheduler.start(addr)
        self.logger.info('Local Directory: %26s', self.local_directory)
        self.logger.info('-' * 47)
#        install_signal_handlers(self.loop)

        try:
            self.loop.start()
            self.loop.close()
        finally:
            self.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()