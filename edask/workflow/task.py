from edask.messageParser import mParse

class Task:

    @staticmethod
    def parse( task_header:str ):
        headerToks = task_header.split('|')
        taskToks = headerToks[1].split('-')
        opToks = taskToks[0].split('.')
        module = ".".join( opToks[0:2] )
        op = opToks[-1]
        rId = taskToks[1]
        inputs = headerToks[2].split(',')
        metadata = mParse.s2m( headerToks[3] )
        return Task( module, op, rId, inputs, metadata )

    def __init__( self, module: str, op: str, rId: str, inputs: list[str], metadata: dict[str,str] ):
        self.module = module
        self.op = op
        self.rId = rId
        self.inputs = inputs
        self.metadata = metadata

    def varNames(self):
        return [ inputId.split('-')[0] for inputId in self.inputs ]

