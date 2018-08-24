from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback, logging
import numpy.ma as ma
from edask.workflow.internal.xarray import *
from edask.workflow.module import edasOpManager
from edask.portal.parsers import WpsCwtParser
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

class TestManager:

  def __init__(self):
    self.logger = logging.getLogger()
    self.addresses = {
      "merra2": CreateIPServer + "/reanalysis/MERRA2/mon/atmos/{}.ncml",
      "merra": CreateIPServer + "/reanalysis/MERRA/mon/atmos/{}.ncml",
      "ecmwf": CreateIPServer + "/reanalysis/ECMWF/mon/atmos/{}.ncml",
      "cfsr": CreateIPServer + "/reanalysis/CFSR/mon/atmos/{}.ncml",
      "20crv": CreateIPServer + "/reanalysis/20CRv2c/mon/atmos/{}.ncml",
      "jra": CreateIPServer + "/reanalysis/JMA/JRA-55/mon/atmos/{}.ncml",
    }

  def getAddress(self, model: str, varName: str) -> str:
    return self.addresses[model.lower()].format(varName)

  def testParseExec(self, domains: List[Dict[str, str]], variables: List[Dict[str, str]],
                    operations: List[Dict[str, str]]) -> List[EDASDataset]:
    testRequest = l2s(["domain = " + dl2s(domains), "variable = " + dl2s(variables), "operation = " + dl2s(operations)])
    dataInputs = WpsCwtParser.parseDatainputs(testRequest)
    request: TaskRequest = TaskRequest.new("requestId", "jobId", dataInputs)
    return edasOpManager.buildRequest(request)

  def testExec(self, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]],
               operations: List[Dict[str, Any]]) -> List[EDASDataset]:
    datainputs = {"domain": domains, "variable": variables, "operation": operations}
    request: TaskRequest = TaskRequest.new("requestId", "jobId", datainputs)
    return edasOpManager.buildRequest(request)

  def print(self, results: List[EDASDataset]):
    for result in results:
      for variable in result.inputs:
        result = variable.data.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

  def equals(self, result: EDASDataset, verification_arrays: List[ma.MaskedArray], thresh: float = 0.0001) -> bool:
    for idx, result in enumerate(result.inputs):
      a1 = result.data.to_masked_array(copy=False).flatten()
      a2 = verification_arrays[idx].flatten()
      size = min(a1.size, a2.size)
      diff = (a1[0:size] - a2[0:size]).ptp(0)
      self.logger.info(" ***** Result {}, differs from verification by {}".format(result.name, str(diff)))
      if diff > 2 * thresh: return False
    return True