from typing import Dict, Any, Union, Sequence, List, Set
import logging, random, string
from edask.process.domain import DomainManager, Domain
import copy
from edask.process.source import VariableManager
from edask.process.operation import OperationManager, WorkflowNode
from edask.workflow.data import EDASDataset
from edask.portal.parsers import WpsCwtParser

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

class Job:
  def __init__( self, requestId: str, identifier: str, datainputs: str,  runargs: Dict[str,str], priority: float ):
    self.requestId = requestId
    self.identifier = identifier
    self.dataInputs = WpsCwtParser.parseDatainputs( datainputs )
    self.runargs = runargs
    self.priority = priority
    self.workerIndex = 0

  def copy( self, workerIndex: int ) -> "Job":
      newjob = copy.deepcopy( self )
      newjob.workerIndex = workerIndex
      return newjob

  def getSchedulerParameters(self):
      ops: List[Dict[str,Any]] = self.dataInputs.get("operation")
      sParms = {}
      for op in ops:
          for key, value in op.items():
              keyToks = key.split(":")
              if (len(keyToks) > 1) and keyToks[0] == "scheduler":
                  sParms[ ":".join(keyToks[1:]) ] = value
      return sParms

  @property
  def iterations(self):
      sParms = self.getSchedulerParameters()
      return int( sParms.get("iterations",1) )

class TaskRequest:
    
  @classmethod
  def new( cls, job: Job ):
    logger = logging.getLogger()
    logger.info( "TaskRequest--> process_name: {}, datainputs: {}".format( job.identifier, str( job.dataInputs ) ))
    uid = UID( job.requestId )
    domainManager = DomainManager.new( job.dataInputs.get("domain") )
    variableManager = VariableManager.new( job.dataInputs.get("variable") )
    operationManager = OperationManager.new( job.dataInputs.get("operation"), domainManager, variableManager )
    rv = TaskRequest( uid, job.identifier, operationManager )
    return rv

  @classmethod
  def init( cls, requestId: str, identifier: str, dataInputs: Dict[str,List[Dict[str,Any]]] ):
    logger = logging.getLogger()
    logger.info( "TaskRequest--> process_name: {}, datainputs: {}".format( identifier, str( dataInputs ) ))
    uid = UID( requestId )
    domainManager = DomainManager.new( dataInputs.get("domain") )
    variableManager = VariableManager.new( dataInputs.get("variable") )
    operationManager = OperationManager.new( dataInputs.get("operation"), domainManager, variableManager )
    rv = TaskRequest( uid, identifier, operationManager )
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

  def linkWorkflow(self) -> List[WorkflowNode]:
      self.operationManager.createWorkflow()
      return self.operationManager.getOperations()

  def domain(self, domId: str, offset: str ) -> Domain:
      dom = self.operationManager.getDomain(domId)
      return dom.offset( offset )

  def subset(self, domId: str, dset: EDASDataset ) -> EDASDataset:
      return dset.subset( self.domain( domId ) ) if dset.requiresSubset(domId) else dset

  def __str__(self):
      return "TaskRequest[{}]:\n\t{}".format( self.name, str(self.operationManager) )

  def getResultOperations(self) -> List[WorkflowNode]:
      return self.operationManager.getResultOperations()


      # variableMap: Dict[str, DataContainer], domainMap: Dict[str, DomainContainer],
      #             operations: Sequence[OperationContext] = [], metadata: Dict[str, str] = Dict("id" -> "#META"), user: User = User());
        