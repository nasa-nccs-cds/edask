from typing import Dict, Any, Union, Sequence, List
import zmq, traceback, time, logging, xml, random, string, defusedxml, abc
from edask.process.domain import DomainManager
from edask.process.variable import VariableManager
from edask.process.operation import OperationManager

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
    logger.info( "TaskRequest--> process_name: %s, datainputs: %s".format(process_name, str( datainputs ) ))
    uid = UID(rId)
    domainManager = DomainManager.new( datainputs.get("domain") )
    variableManager = VariableManager.new( datainputs.get("variable") )
    operationManager = OperationManager.new( datainputs.get("operation"), domainManager, variableManager )
    # op_spec_list: Sequence[Dict[str, Any]] = datainputs .get( "operation", [] )
    # data_list: List[DataContainer] = datainputs.get("variable", []).flatMap(DataContainer.factory(uid, _, op_spec_list.isEmpty )).toList
    # domain_list: List[DomainContainer] = datainputs.get("domain", []).map(DomainContainer(_)).toList
    # opSpecs: Sequence[Dict[str, Any]] = if(op_spec_list.isEmpty) { getEmptyOpSpecs(data_list) } else { op_spec_list }
    # operation_map: Dict[str,OperationContext] = Dict( opSpecs.map (  op => OperationContext( uid, process_name, data_list.map(_.uid), op) ) map ( opc => opc.identifier -> opc ) :_* )
    # operation_list: Sequence[OperationContext] = operation_map.values.toSeq
    # variableMap: Dict[str, DataContainer] = buildVarMap(data_list, operation_list)
    # domainMap: Dict[str, DomainContainer] = buildDomainMap(domain_list)
    # inferDomains(operation_list, variableMap )
    # gridId = datainputs.get("grid", data_list.headOption.map(dc => dc.uid).get("#META")).toString
    # gridSpec = Dict("id" -> gridId.toString)
    rv = TaskRequest( uid, process_name, operationManager )
    logger.info( " -> Generated TaskRequest, uid = " + str(uid) )
    return rv

  
  
  def __init__( self, id: UID, name: str, _operationManager: OperationManager ):
      self.uid = id
      self.name = name
      self.operationManager = _operationManager

  def linkWorkflow(self):
      self.operationManager.createWorkflow()

  def __str__(self):
      return "TaskRequest[{}]:\n\t{}".format( self.name, str(self.operationManager) )


      # variableMap: Dict[str, DataContainer], domainMap: Dict[str, DomainContainer],
      #             operations: Sequence[OperationContext] = [], metadata: Dict[str, str] = Dict("id" -> "#META"), user: User = User());
        