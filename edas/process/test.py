import logging, time, multiprocessing
import numpy.ma as ma
from edas.process.task import Job
from edas.workflow.modules.edas import *
from edas.workflow.module import edasOpManager
from edas.util.logging import EDASLogger
from edas.process.manager import ProcessManager, ExecHandler
from edas.config import EdasEnv
from edas.portal.cluster import EDASCluster
from typing import List, Optional, Tuple, Dict, Any
from threading import Thread

CreateIPServer = "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/"


def q(item: str):
    i0 = item.strip()
    return i0 if i0.startswith('"') or i0.startswith('{') else '"' + i0 + '"'


def l2s(items: List[str]) -> str:
    return "[ " + ", ".join(items) + " ]"


def dl2s(items: List[Dict[str, str]]) -> str:
    return "[ " + ", ".join([d2s(i) for i in items]) + " ]"


def d2s(dict: Dict[str, str]) -> str:
    return "{ " + ", ".join([q(k) + ":" + q(v) for (k, v) in dict.items()]) + " }"


class TestDataManager:
    addresses = {
        "merra2": CreateIPServer + "/reanalysis/MERRA2/mon/atmos/{}.ncml",
        "merra2-day": CreateIPServer + "/reanalysis/MERRA2/day/atmos/{}.ncml",
        "merra2-6hr": CreateIPServer + "/reanalysis/MERRA2/6hr/atmos/{}.ncml",
        "merra": CreateIPServer + "/reanalysis/MERRA/mon/atmos/{}.ncml",
        "ecmwf": CreateIPServer + "/reanalysis/ECMWF/mon/atmos/{}.ncml",
        "cfsr": CreateIPServer + "/reanalysis/CFSR/mon/atmos/{}.ncml",
        "20crv": CreateIPServer + "/reanalysis/20CRv2c/mon/atmos/{}.ncml",
        "jra": CreateIPServer + "/reanalysis/JMA/JRA-55/mon/atmos/{}.ncml",
    }

    @classmethod
    def getAddress(cls, model: str, varName: str) -> str:
        return cls.addresses[model.lower()].format(varName)


class TestManager:

    def __init__(self, _proj: str, _exp: str):
        self.logger = EDASLogger.getLogger()
        self.project = _proj
        self.experiment = _exp

    def getAddress(self, model: str, varName: str) -> str:
        return TestDataManager.getAddress(model, varName)

    def getVar(self, collection: str, varName: str, id: str, domain: str):
        return {"uri": self.getAddress(collection, varName), "name": varName + ":" + id, "domain": domain}

    def print(self, results: List[EDASDataset]):
        for result in results:
          for variable in result.inputs:
            result = variable.xr.load()
            self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
            self.logger.info(result)

    def equals(self, result: EDASDataset, verification_arrays: List[ma.MaskedArray], thresh: float = 0.0001) -> bool:
        if result is None: return False
        for idx, result in enumerate(result.inputs):
            a1 = result.xr.to_masked_array(copy=False).flatten()
            a2 = verification_arrays[idx].flatten()
            size = min(a1.size, a2.size)
            diff = (a1[0:size] - a2[0:size]).ptp(0)
            self.logger.info(" ***** Result {}, differs from verification by {}".format(result.name, str(diff)))
            if diff > 2 * thresh: return False
        return True


class LocalTestManager(TestManager):

    def __init__(self, _proj: str, _exp: str, appConf: Dict[str,str] = None):
        super(LocalTestManager, self).__init__(_proj, _exp)
        EdasEnv.update(appConf)

    def testExec(self, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]], processResult: bool = True ) -> List[EDASDataset]:
        t0 = time.time()
        runArgs = dict( ncores = multiprocessing.cpu_count() )
        job = Job.init( self.project, self.experiment, "jobId", domains, variables, operations, [], runArgs )
        datainputs = {"domain": domains, "variable": variables, "operation": operations}
        resultHandler = ExecHandler( "testJob", job )
        request: TaskRequest = TaskRequest.init(self.project, self.experiment, "requestId", "jobId", datainputs)
        results: List[EDASDataset] = edasOpManager.buildRequest(request)
        if processResult:
            for result in results: resultHandler.processResult(result)
        self.logger.info( " Completed computation in " + str( time.time() - t0 ) + " seconds")
        return results

class DistributedTestManager(TestManager):

    def __init__(self, _proj: str, _exp: str, appConf: Dict[str,str] = None):
        super(DistributedTestManager, self).__init__(_proj, _exp)
        EdasEnv.update(appConf)
        log_metrics = appConf.get("log_metrics",True)
        self.processManager = ProcessManager( EdasEnv.parms )
        time.sleep(10)
        self.processing = False
        self.scheduler_info = self.processManager.client.scheduler_info()
        self.workers: Dict = self.scheduler_info.pop("workers")
        self.logger.info(" @@@@@@@ SCHEDULER INFO: " + str(self.scheduler_info ))
        self.logger.info(f" N Workers: {len(self.workers)} " )
        for addr, specs in self.workers.items():
            self.logger.info(f"  -----> Worker {addr}: {specs}" )
        if log_metrics:
            self.metricsThread =  Thread( target=self.trackCwtMetrics )
            self.metricsThread.start()

    def trackCwtMetrics(self, sleepTime=1.0):
        isIdle = False
        self.logger.info(f" ** TRACKING CWT METRICS ** ")
        while True:
            metrics = self.getCWTMetrics()
            counts = metrics['user_jobs_running']
            if counts['processing'] == 0:
                if not isIdle:
                    self.logger.info(f" ** CLUSTER IS IDLE ** ")
                    isIdle = True
            else:
                isIdle = False
                self.logger.info("   ------------------------- CWT METRICS  -------------------  ------------------------ ")
                for key, value in metrics.items():
                    self.logger.info(f" *** {key}: {value}")
                self.logger.info("   ----------------------- -----------------------------------  ----------------------- ")
                time.sleep(sleepTime)

    def getCWTMetrics(self) -> Dict:
        metrics_data = self.processManager.getCWTMetrics()
        metrics_data['wps_requests'] = 1 if self.processing else 0
        return metrics_data

    @property
    def ncores(self):
        sample_worker = list(self.workers.values())[0]
        return sample_worker["ncores"]

    def testExec(self, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]]) ->  List[EDASDataset]:
        runArgs = dict( ncores=self.ncores )
        job = Job.init( self.project, self.experiment, "jobId", domains, variables, operations, [], runArgs )
        execHandler = ExecHandler("local", job, workers=job.workers)
        self.processing = True
        execHandler.execJob( job )
        result = execHandler.getEDASResult()
        self.processing = False
        return result



