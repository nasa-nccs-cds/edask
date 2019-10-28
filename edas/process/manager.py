from typing import Dict, Any, Union, List, Callable, Optional
import zmq, traceback, time, logging, xml, socket, abc, dask, threading, requests, json
from edas.workflow.module import edasOpManager
from edas.process.task import Job
from edas.workflow.data import EDASDataset
from dask.distributed import Client, Future, LocalCluster
from stratus_endpoint.handler.base import Status
from dask_jobqueue import SLURMCluster
from edas.util.logging import EDASLogger
from edas.portal.cluster import EDASCluster
from edas.config import EdasEnv
import random, string, os, queue, datetime, atexit, multiprocessing, errno, uuid
from threading import Thread
import xarray as xa

class ExecHandlerBase:
    __metaclass__ = abc.ABCMeta

    def __init__(self, clientId: str, jobId: str, **kwargs ):
        self.logger = EDASLogger.getLogger()
        self.clientId = clientId
        self.jobId = jobId
        self.cacheDir = kwargs.get( "cache", "/tmp")
        self.workers = kwargs.get( "workers", 1 )
        self.start_time = time.time()
        self.filePath = self.cacheDir + "/" + Job.randomStr(6) + ".nc"

    def updateStartTime( self):
        self.start_time = time.time()

    def getExpDir(self, proj: str, exp: str ) -> str:
        expDir =  self.cacheDir + "/experiments/" + proj + "/" + exp
        return self.mkDir( expDir )

    def getExpFile(self, proj: str, exp: str, name: str, type: str = "nc" ) -> str:
        expDir =  self.getExpDir( proj, exp )
        return expDir + "/" + name + "-" + Job.randomStr(6) + "." + type

    @abc.abstractmethod
    def processFailure(self, ex: Exception): pass

    @abc.abstractmethod
    def processResult(self, result: EDASDataset  ): pass

    def mkDir(self, dir: str ) -> str:
        try:
            os.makedirs(dir)
        except OSError as e:
            if e.errno != errno.EEXIST: raise
        return dir

class SubmissionThread(Thread):

    def __init__(self, job: Job, processResults, processFailure ):
        Thread.__init__(self)
        self.job = job
        self.processResults = processResults
        self.processFailure = processFailure
        self.logger =  EDASLogger.getLogger()

    def run(self):
        start_time = time.time()
        try:
            self.logger.info( "* Running workflow for requestId " + self.job.requestId)
            results: List[EDASDataset] = edasOpManager.buildTask( self.job )
            self.logger.info( "Completed edas workflow in time " + str(time.time()-start_time) )
            self.processResults( results )
        except Exception as err:
            self.logger.error( "Execution error: " + str(err))
            self.logger.error( traceback.format_exc() )
            self.processFailure(err)

class ExecHandler(ExecHandlerBase):

    def __init__( self, clientId: str, _job: Job, portal = None, **kwargs ):
        from edas.portal.base import EDASPortal
        super(ExecHandler, self).__init__(clientId, _job.requestId, **kwargs)
        self.portal: EDASPortal = portal
        self.sthread = None
        self._processResults = True
        self.results: List[EDASDataset] = []
        self.job = _job
        self._status = Status.IDLE
        self._parms = {}

    def start(self) -> SubmissionThread:
        self.sthread = SubmissionThread( self.job, self.processResults, self.processFailure )
        self.sthread.start()
        self.logger.info( " ----------------->>> Submitted request for job " + self.job.requestId )
        return self.sthread

    def status(self):
        return self._status

    def getEDASResult(self, timeout=None, block=False) -> List[EDASDataset]:
        self._processResults = False
        if block:
            self.sthread.join(timeout)
            return self.mergeResults()
        else:
            if self._status == Status.COMPLETED:
                return self.mergeResults()
            else: return []

    def getResult(self, timeout=None, block=False) ->  List[xa.Dataset]:
        edasResults = self.getEDASResult(timeout,block)
        return [ edasResult.xr for edasResult in edasResults ] if edasResults is not None else []

    def processResults( self, results: List[EDASDataset] ):
        self.results.extend( results )
        self._processFinalResults( )
        if self.portal: self.portal.removeHandler( self.clientId, self.jobId )
        self._status = Status.COMPLETED
        self.logger.info(" ----------------->>> EDAS REQUEST COMPLETED, result Len =  " + str(len(self.results))  )

    def _processFinalResults( self ):
        assert len(self.results), "No results generated by request"
        if self._processResults:
            self.logger.info(" ----------------->>> Process Final Result " )
            results: List[EDASDataset] = self.mergeResults()
            for result in results:
                try:
                    savePath = result.save()
                    if self.portal:
                        sendData = self.job.runargs.get( "sendData", "true" ).lower().startswith("t")
                        self.portal.sendFile( self.clientId, self.jobId, result.id, savePath, sendData )
                    else:
                        self.printResult(savePath)
                except Exception as err:
                    self.logger.error( "Error processing final result: " + str(err) )
                    self.logger.info(traceback.format_exc())
                    if self.portal:
                        self.portal.sendFile(self.clientId, self.jobId, result.id, "", False )

    def printResult( self, filePath: str ):
        dset = xa.open_dataset(filePath)
        print( str(dset) )

    def mergeResults(self) -> List[EDASDataset]:
        if self.results[0].getResultClass() == "METADATA":
            return self.results
        mergeMethod: str = self.results[0]["merge"]
        if mergeMethod is None:
            return EDASDataset.merge( self.results )
        mergeToks = mergeMethod.split(":")
        return self.getBestResult( mergeToks[0].strip().lower(), mergeToks[1].strip().lower() )

    def getBestResult(self, method: str, parm: str )-> List[EDASDataset]:
        bestResult = None
        bestValue = None
        values = []
        for result in self.results:
            pval = result[parm]
            assert pval, "Error, parameter '{}' not defined in dataset".format( parm )
            values.append(float(pval))
            if bestResult is None or self.compare( method, float(pval), bestValue ):
                bestResult = result
                bestValue = float(pval)
        return [ bestResult ]

    def compare(self, method: str, current: float, threshold: float ):
        if method == "min": return current < threshold
        if method == "max": return current > threshold
        raise Exception( "Unknown comparison method: " + method )

    @classmethod
    def getTbStr( cls, ex ) -> str:
        if ex.__traceback__  is None: return ""
        tb = traceback.extract_tb( ex.__traceback__ )
        return " ".join( traceback.format_list( tb ) )

    @classmethod
    def getErrorReport( cls, ex ):
        try:
            errMsg = getattr( ex, 'message', repr(ex) )
            return errMsg + ">~>" +  str( cls.getTbStr(ex) )
        except:
            return repr(ex)

    def processFailure(self, ex: Exception):
        error_message = self.getErrorReport( ex )
        if self.portal:
            self.portal.sendErrorReport( self.clientId, self.jobId, error_message )
            self.portal.removeHandler( self.clientId, self.jobId )
        else:
            self.logger.error( error_message )
        self._status = Status.ERROR
        self._parms["error"] = error_message

    # def iterationCallback( self, resultFuture: Future ):
    #   status = resultFuture.status
    #   if status == "finished":
    #       self.completed = self.completed + 1
    #       result: EDASDataset = resultFuture.result()
    #       self.results.append(result)
    #   else:
    #       try:                      self.processFailure(Exception("status = " + status + "\n>~>" + str(traceback.format_tb(resultFuture.traceback(60)))))
    #       except TimeoutError:
    #           try:                  self.processFailure(Exception("status = " + status + ", Exception = " + str(resultFuture.exception(60))))
    #           except TimeoutError:
    #                                 self.processFailure(Exception("status = " + status))
    #   if self.completed == self.workers:
    #     self._processFinalResult()
    #     if self.portal:
    #         self.portal.removeHandler( self.clientId, self.jobId )

class GenericProcessManager:
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def submitProcess(self, service: str, job: Job, resultHandler: ExecHandler)-> str: pass

  @abc.abstractmethod
  def getResult( self, service: str, resultId: str ): pass

  @abc.abstractmethod
  def getResultStatus( self, service: str, resultId: str ): pass

  @abc.abstractmethod
  def hasResult( self, service: str, resultId: str )-> bool: pass

  @abc.abstractmethod
  def serverIsDown( self )-> bool: pass

  @abc.abstractmethod
  def term(self): pass

  def waitUntilJobCompletes( self, service: str, resultId: str  ):
    while( not self.hasResult(service,resultId) ): time.sleep(0.5)

class ProcessManager(GenericProcessManager):
  manager: "ProcessManager" = None

  @classmethod
  def getManager( cls ) -> Optional["ProcessManager"]:
      return cls.manager

  @classmethod
  def initManager( cls, serverConfiguration: Dict[str,str] ) -> "ProcessManager":
      if cls.manager is None:
          cls.manager = ProcessManager(serverConfiguration)
      return cls.manager

  def __init__( self, serverConfiguration: Dict[str,str] ):
      self.config = serverConfiguration
      self.logger =  EDASLogger.getLogger()
      self.num_wps_requests = 0
      self.scheduler_address = serverConfiguration.get("scheduler.address",None)
      self.submitters = []
      self.slurm_clusters = {}
      self.active = True
      if self.scheduler_address is not None:
          self.logger.info( "Initializing Dask-distributed cluster with scheduler address: " + self.scheduler_address )
          self.client = Client( self.scheduler_address, timeout=63 )
      elif self.scheduler_address.lower().startswith("slurm"):
          scheduler_parms = self.scheduler_address.split(":")
          queue = "default" if len( scheduler_parms ) < 2 else scheduler_parms[1]
          cluster = self.getSlurmCluster( queue )
          self.client = Client( cluster )
      else:
          nWorkers = int( self.config.get("dask.nworkers",multiprocessing.cpu_count()) )
          self.client = Client( LocalCluster( n_workers=nWorkers ) )
          self.scheduler_address = self.client.scheduler.address
          self.logger.info( f"Initializing Local Dask cluster with {nWorkers} workers,  scheduler address = {self.scheduler_address}")
          self.client.submit( lambda x: edasOpManager.buildIndices( x ), nWorkers )
      self.ncores = self.client.ncores()
      self.logger.info(f" ncores: {self.ncores}")
      self.scheduler_info = self.client.scheduler_info()
      self.workers: Dict = self.scheduler_info.pop("workers")
      self.logger.info(f" workers: {self.workers}")
      log_metrics = serverConfiguration.get("log.scheduler.metrics", False )
      if log_metrics:
        self.metricsThread =  Thread( target=self.trackMetrics )
        self.metricsThread.start()

  def getSlurmCluster( self, queue: str ):
      self.logger.info( f"Initializing Slurm cluster using queue {queue}" )
      return self.slurm_clusters.setdefault( queue, SLURMCluster() if queue == "default" else SLURMCluster( queue=queue ) )

  def getCWTMetrics(self) -> Dict:
      metrics_data = { key:{} for key in ['user_jobs_queued','user_jobs_running','wps_requests','cpu_ave','cpu_count','memory_usage','memory_available']}
      metrics = self.getProfileData()
      counts = metrics["counts"]
      workers = metrics["workers"]
      for key in ['tasks','processing','released','memory','saturated','waiting','waiting_data','unrunnable']: metrics_data['user_jobs_running'][key] = counts[key]
      for key in ['tasks', 'waiting', 'waiting_data', 'unrunnable']: metrics_data['user_jobs_queued'][key] = counts[key]
      for wId, wData in workers.items():
          worker_metrics = wData["metrics"]
          total_memory   = wData["memory_limit"]
          memory_usage = worker_metrics["memory"]
          metrics_data['memory_usage'][wId] = memory_usage
          metrics_data['memory_available'][wId] = total_memory - memory_usage
          metrics_data['cpu_count'][wId] = wData["ncores"]
          metrics_data['cpu_ave'][wId] = worker_metrics["cpu"]
      return metrics_data

  def trackMetrics(self, sleepTime=1.0 ):
      isIdle = False
      self.logger.info(f" ** TRACKING METRICS ** ")
      while self.active:
          metrics = self.getProfileData()
          counts = metrics["counts"]
          if counts['processing'] == 0:
              if not isIdle:
                self.logger.info(f" ** CLUSTER IS IDLE ** ")
                isIdle = True
          else:
              isIdle = False
              self.logger.info( f" METRICS: {metrics['counts']} " )
              workers = metrics["workers"]
              for key,value in workers.items():
                  self.logger.info( f" *** {key}: {value}" )
              self.logger.info(f" HEALTH: {self.getHealth()}")
              time.sleep( sleepTime )

  def getWorkerMetrics(self):
      metrics = {}
      wkeys = [ 'ncores', 'memory_limit', 'last_seen', 'metrics' ]
      scheduler_info = self.client.scheduler_info()
      workers: Dict = scheduler_info.get( "workers", {} )
      for iW, worker in enumerate( workers.values() ):
          metrics[f"W{iW}"] = { wkey: worker[wkey] for wkey in wkeys }
      return metrics

  def getDashboardAddress(self):
      stoks = self.scheduler_address.split(":")
      host_address = stoks[-2].strip("/")
      return f"http://{host_address}:8787"

  def getCounts(self) -> Dict:
      profile_address = f"{self.getDashboardAddress()}/json/counts.json"
      return requests.get(profile_address).json()

  def getHealth(self, mtype: str = "" ) -> str:
      profile_address = f"{self.getDashboardAddress()}/health"
      return requests.get(profile_address).text

  def getMetrics(self, mtype: str = "" ) -> Optional[Dict]:
      counts = self.getCounts()
      if counts['processing'] == 0: return None
      mtypes = mtype.split(",")
      metrics = { "counts": counts }
      if "processing" in mtypes:  metrics["processing"] = self.client.processing()
      if "profile" in mtypes:     metrics["profile"]    = self.client.profile()
      return metrics

  def getProfileData( self, mtype: str = "" ) -> Dict:
      try:
        return { "counts": self.getCounts(), "workers": self.getWorkerMetrics() }
      except Exception as err:
          self.logger.error( "Error in getProfileData")
          self.logger.error(traceback.format_exc())

      # response2: requests.Response = requests.get(tasks_address)
      # print(f"\n  ---->  Tasks Data from {tasks_address}: \n **  {response2.text} ** \n" )
      # response3: requests.Response = requests.get(workers_address)
      # print(f"\n  ---->  Workers Data from {workers_address}: \n **  {response3.text} ** \n" )

#      data = json.loads(counts)

    # (r"info/main/workers.html", Workers),
    # (r"info/worker/(.*).html", Worker),
    # (r"info/task/(.*).html", Task),
    # (r"info/main/logs.html", Logs),
    # (r"info/call-stacks/(.*).html", WorkerCallStacks),
    # (r"info/call-stack/(.*).html", TaskCallStack),
    # (r"info/logs/(.*).html", WorkerLogs),
    # (r"json/counts.json", CountsJSON),
    # (r"json/identity.json", IdentityJSON),
    # (r"json/index.html", IndexJSON),
    # (r"individual-plots.json", IndividualPlots),
    # (r"metrics", PrometheusHandler),
    # (r"health", HealthHandler),

  # "/system": systemmonitor_doc,
  # "/stealing": stealing_doc,
  # "/workers": workers_doc,
  # "/events": events_doc,
  # "/counters": counters_doc,
  # "/tasks": tasks_doc,
  # "/status": status_doc,
  # "/profile": profile_doc,
  # "/profile-server": profile_server_doc,
  # "/graph": graph_doc,
  # "/individual-task-stream": individual_task_stream_doc,
  # "/individual-progress": individual_progress_doc,
  # "/individual-graph": individual_graph_doc,
  # "/individual-profile": individual_profile_doc,
  # "/individual-profile-server": individual_profile_server_doc,
  # "/individual-nbytes": individual_nbytes_doc,
  # "/individual-nprocessing": individual_nprocessing_doc,
  # "/individual-workers": individual_workers_doc,

  def term(self):
      self.active = False
      self.client.close()

  def runProcess( self, job: Job ) -> EDASDataset:
    start_time = time.time()
    try:
        self.logger.info( f"Running workflow for requestId: {job.requestId}, scheduler: {self.scheduler_address}" )
        result = edasOpManager.buildTask( job )
        self.logger.info( "Completed EDAS workflow in time " + str(time.time()-start_time) )
        return result
    except Exception as err:
        self.logger.error( "Execution error: " + str(err))
        traceback.print_exc()


  def submitProcess(self, service: str, job: Job, resultHandler: ExecHandler):
      submitter: SubmissionThread = SubmissionThread( job, resultHandler )
      self.submitters.append( submitter )
      submitter.start()

if __name__ == '__main__':
    cluster = SLURMCluster()


