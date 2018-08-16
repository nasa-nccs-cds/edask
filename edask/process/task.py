from typing import Dict, Any, Union, Sequence, List
import zmq, traceback, time, logging, xml, random, string, defusedxml, abc
from edask.process.domain import DomainManager, Domain
import xarray as xr
from edask.process.source import VariableManager
from edask.process.operation import OperationManager, WorkflowNode
from edask.process.domain import Axis, AxisBounds

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

  def linkWorkflow(self):
      self.operationManager.createWorkflow()

  def getIndexers(self, domain: str, dataset: xr.Dataset ) -> List[xr.DataArray]:     # TODO - complete
      domain: Domain = self.operationManager.getDomain( domain )
      axisBounds: Dict[Axis,AxisBounds] = domain.axisBounds
      indexers: List[xr.DataArray] = []
      for ( axis, bounds ) in axisBounds.items():
          if bounds.system.startswith("val"):
            pass
          else:
            indexers.append( xr.DataArray( range( int(bounds.start), int(bounds.end) ), dims=[axis.name] ) )
      return indexers


  def __str__(self):
      return "TaskRequest[{}]:\n\t{}".format( self.name, str(self.operationManager) )

  def getResultOperations(self) -> List[WorkflowNode]:
      return self.operationManager.getResultOperations()


      # variableMap: Dict[str, DataContainer], domainMap: Dict[str, DomainContainer],
      #             operations: Sequence[OperationContext] = [], metadata: Dict[str, str] = Dict("id" -> "#META"), user: User = User());
        