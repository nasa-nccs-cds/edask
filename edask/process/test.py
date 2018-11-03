import logging, time
import numpy.ma as ma
from edask.process.task import Job
from edask.workflow.modules.xarray import *
from edask.workflow.module import edasOpManager
from edask.util.logging import EDASLogger
from edask.process.manager import ProcessManager, ExecResultHandler
from edask.config import EdaskEnv

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

    def testParseExec(self, domains: List[Dict[str, str]], variables: List[Dict[str, str]], operations: List[Dict[str, str]]) -> EDASDataset:
        testRequest = l2s(["domain = " + dl2s(domains), "variable = " + dl2s(variables), "operation = " + dl2s(operations)])
        job = Job.new("requestId", "jobId", testRequest)
        return edasOpManager.buildTask(job)

    def print(self, results: EDASDataset):
        for variable in results.inputs:
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

    def __init__(self, _proj: str, _exp: str):
        super(LocalTestManager, self).__init__(_proj, _exp)

    def testExec(self, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]], processResult: bool = True ) -> EDASDataset:
        t0 = time.time()
        datainputs = {"domain": domains, "variable": variables, "operation": operations}
        resultHandler = ExecResultHandler("testJob", "local")
        request: TaskRequest = TaskRequest.init(self.project, self.experiment, "requestId", "jobId", datainputs)
        result = edasOpManager.buildRequest(request)
        if processResult: resultHandler.processResult(result)
        self.logger.info( " Completed computation in " + str( time.time() - t0 ) + " seconds")
        return result

class DistributedTestManager(TestManager):

    def __init__(self, _proj: str, _exp: str, appConfiguration=None):
        super(DistributedTestManager, self).__init__(_proj, _exp)
        if appConfiguration is None:
            appConfiguration = {}
        self.processManager = ProcessManager({**EdaskEnv.parms, **appConfiguration})

    def testExec(self, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]]) -> ExecResultHandler:
        job = Job.init( self.project, self.experiment, "jobId", domains, variables, operations )
        resultHandler = ExecResultHandler( "local", job.process, workers=job.workers)
        self.processManager.executeProcess(job.process, job, resultHandler)
        return resultHandler
