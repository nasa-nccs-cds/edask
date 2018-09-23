from typing import Dict, Any, Union, Sequence, List, Set, Optional, Iterable
import logging, random, string
import xarray as xa
from edask.process.domain import DomainManager, Domain, AxisBounds, Axis
import copy
from edask.process.source import VariableManager
from edask.process.operation import OperationManager, WorkflowNode
from edask.portal.parsers import WpsCwtParser
from edask.workflow.data import EDASDataset, EDASArray
from edask.collections.agg import Archive

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

  def __init__(self, requestId: str, project: str, experiment: str, process: str, datainputs: Dict[str, List[Dict[str, Any]]], runargs: Dict[str, str], priority: float):
        self.requestId = requestId
        self.process = process
        self.project = project
        self.experiment = experiment
        self.dataInputs = datainputs
        self.runargs = runargs
        self.priority = priority
        self.workerIndex = 0

  @staticmethod
  def new( requestId: str, project: str, experiment: str, process: str, datainputs: str,  runargs: Dict[str,str], priority: float ):
    return Job( requestId, project, experiment, process, WpsCwtParser.parseDatainputs( datainputs ), runargs, priority)

  @staticmethod
  def randomStr(length) -> str:
      tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
      return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

  @classmethod
  def init( cls, project: str, experiment: str, process: str, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]],  runargs: Dict[str,str]={}, priority: float=0.0 ):
    return Job( cls.randomStr(6), project, experiment, process, { "domain":domains, "variable":variables, "operation":operations }, runargs, priority )

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
  def workers(self):
      sParms = self.getSchedulerParameters()
      return int( sParms.get("workers",1) )

class TaskRequest:
    
  @classmethod
  def new( cls, job: Job ):
    logger = logging.getLogger()
    logger.info( "TaskRequest--> process_name: {}, datainputs: {}".format(job.process, str(job.dataInputs)))
    uid = UID( job.requestId )
    domainManager = DomainManager.new( job.dataInputs.get("domain") )
    variableManager = VariableManager.new( job.dataInputs.get("variable") )
    operationManager = OperationManager.new( job.dataInputs.get("operation"), domainManager, variableManager )
    rv = TaskRequest(uid, job.project, job.experiment, job.process, operationManager)
    return rv

  @classmethod
  def init( cls, project: str, experiment: str, requestId: str, identifier: str, dataInputs: Dict[str,List[Dict[str,Any]]] ):
    logger = logging.getLogger()
    logger.info( "TaskRequest--> process_name: {}, datainputs: {}".format( identifier, str( dataInputs ) ))
    uid = UID( requestId )
    domainManager = DomainManager.new( dataInputs.get("domain") )
    variableManager = VariableManager.new( dataInputs.get("variable") )
    operationManager = OperationManager.new( dataInputs.get("operation"), domainManager, variableManager )
    rv = TaskRequest( uid, project, experiment, identifier, operationManager )
    return rv

  def __init__( self, id: UID, project: str, experiment: str, name: str, _operationManager: OperationManager ):
      self.uid = id
      self.name = name
      self.project = project
      self.experiment = experiment
      self.operationManager = _operationManager
      self._resultCache: Dict[str, EDASDataset] = {}

  def getCachedResult( self, key: str )-> EDASDataset:
      return self._resultCache.get( key )

  def cacheResult(self, key: str, result: EDASDataset)-> "TaskRequest":
      self._resultCache[ key ] = result
      return self

  def intersectDomains(self, domainIds = Set[str], allow_broadcast: bool = True  ) -> str:
      return self.operationManager.domains.intersectDomains( domainIds, allow_broadcast )

  def cropDomain( self, domainId: str, inputs: Iterable[EDASArray], offset: Optional[str] = None ) -> Domain:
      dom: Domain = self.operationManager.getDomain( domainId )
      domain = dom if offset is None else dom.offset( offset )
      new_domain = Domain( domain.name, {} )
      for axis,bound in domain.axisBounds.items():
          new_domain.addBounds( axis, self.cropBounds(axis,bound,inputs) )
      return new_domain

  def cropBounds( self, axis: Axis, bound: AxisBounds, inputs: Iterable[EDASArray] ) -> AxisBounds:
      new_bounds: AxisBounds = bound
      for input in inputs:
        coord: xa.DataArray = input.coord(axis)
        if coord is not None:
            assert len( coord.shape ) == 1, "Not currently supporting multi-dimensional axes: " + coord.name
            values = coord.values
            new_bounds = new_bounds.crop( axis, values[0], values[-1] )
      return new_bounds

  def linkWorkflow(self) -> List[WorkflowNode]:
      self.operationManager.createWorkflow()
      return self.operationManager.getOperations()

  def domain(self, domId: str, offset: Optional[str] ) -> Domain:
      dom = self.operationManager.getDomain(domId)
      return dom.offset( offset )

  def subset(self, domId: str, dset: EDASDataset ) -> EDASDataset:
      return dset.subset( self.domain( domId ) ) if dset.requiresSubset(domId) else dset

  def __str__(self):
      return "TaskRequest[{}]:\n\t{}".format( self.name, str(self.operationManager) )

  def getResultOperations(self) -> List[WorkflowNode]:
      return self.operationManager.getResultOperations()

  def archivePath(self, id: str = None) -> str:
      toks = id.split("/")
      if len(toks) == 3: return Archive.getFilePath( toks[0], toks[1], toks[2] )
      elif len(toks) == 2: return Archive.getFilePath( self.project, toks[0], toks[1] )
      elif len(toks) == 1: return Archive.getFilePath( self.project, self.experiment, toks[0] )
      else: raise Exception( "Poorly formed archive id: " + str( id ) )


      # variableMap: Dict[str, DataContainer], domainMap: Dict[str, DomainContainer],
      #             operations: Sequence[OperationContext] = [], metadata: Dict[str, str] = Dict("id" -> "#META"), user: User = User());
        