from typing import Dict, Any, Union, Sequence, List, Set
import zmq, traceback, time, logging, xml, random, string, defusedxml, abc
from edask.process.domain import DomainManager, Domain
import xarray as xr
from edask.process.source import VariableManager
from edask.process.operation import OperationManager, WorkflowNode
from edask.workflow.results import KernelSpec, EDASDataset, EDASArray

class UID:
    ndigits = 6

    @staticmethod
    def randomId( length: int ) -> str:
        sample = string.ascii_lowercase+string.digits+string.ascii_uppercase
        return ''.join(random.choice(sample) for i in range(length))

    def __init__(self, uid = None ):
        self.uid = uid if uid else self.randomId( UID.ndigits )

    def __add__(self, other: str ):
        return other if other.endswith(self.uid) else other + "-" + self.uid

    def __str__(self): return self.uid

class TaskRequest:
    
  @classmethod
  def new( cls, rId: str, process_name: str, datainputs: Dict[str, List[Dict[str, Any]]]):
    logger = logging.getLogger()
    logger.info( "TaskRequest--> process_name: {}, datainputs: {}".format(process_name, str( datainputs ) ))
    uid = UID(rId)
    domainManager = DomainManager.new( datainputs.get("domain") )
    variableManager = VariableManager.new( datainputs.get("variable") )
    operationManager = OperationManager.new( datainputs.get("operation"), domainManager, variableManager )
    rv = TaskRequest( uid, process_name, operationManager )
    return rv

  def __init__( self, id: UID, name: str, _operationManager: OperationManager ):
      self.uid = id
      self.name = name
      self.operationManager = _operationManager
      self._resultCache: Dict[str, EDASDataset] = {}

  def getCachedResult( self, key: str )-> EDASDataset:
      return self._resultCache.get( key )

  def cacheResult(self, key: str, result: EDASDataset)-> "TaskRequest":
      self._resultCache[ key ] = result
      return self

  def intersectDomains(self, domainIds = Set[str], allow_broadcast: bool = True  ) -> str:
      return self.operationManager.domains.intersectDomains( domainIds, allow_broadcast )

  def linkWorkflow(self):
      self.operationManager.createWorkflow()

  def subset(self, domain: str, dataset: xr.Dataset) -> xr.Dataset:
      domain: Domain = self.operationManager.getDomain( domain )
      return domain.subset( dataset )

  def subsetDataset(self, domainId: str, kernelResult: EDASDataset ) -> EDASDataset:
      if not kernelResult.requiresSubset(domainId): return kernelResult
      domain: Domain = self.operationManager.getDomain( domainId )
      new_dataset = domain.subset( kernelResult.dataset )
      varList = { vid:domainId for vid in kernelResult.ids }
      return EDASDataset( new_dataset, varList )

  def subsetArray(self, domainId: str, input: EDASArray ) -> EDASArray:
      domain: Domain = self.operationManager.getDomain( domainId )
      return EDASArray( domainId, domain.subsetArray( input.data ) )

  def __str__(self):
      return "TaskRequest[{}]:\n\t{}".format( self.name, str(self.operationManager) )

  def getResultOperations(self) -> List[WorkflowNode]:
      return self.operationManager.getResultOperations()


      # variableMap: Dict[str, DataContainer], domainMap: Dict[str, DomainContainer],
      #             operations: Sequence[OperationContext] = [], metadata: Dict[str, str] = Dict("id" -> "#META"), user: User = User());
        