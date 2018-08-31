import os, sys
import cdms2 as cdms

PC = 0
EOF = 1

class Project:

    def __init__(self, baseDir, _name ):
        self.name = _name

        self.exts = { PC: '-PCs.nc', EOF: '-EOFs.nc' }
        self.directory = os.path.join( baseDir, _name )
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def outfilePath(self, experiment, rType ):
        # type: (str, int) -> str
        return os.path.join( self.directory, experiment + self.exts.get( rType, "Invalid response type: " + str(rType) ) )

    def getVariableNames(self, experiment, rType ):
        # type: (str, int) -> list[str]
        dataset = self.getDataset( experiment, rType )
        return [ vname for vname in dataset.listvariables() if not vname.endswith("_bnds") ]

    def getDataset(self, experiment, rType ):
        # type: (str, int) -> cdms.dataset.CdmsFile
         filePath = self.outfilePath( experiment, rType )
         return cdms.open(filePath)

    def getVariable( self, varName, experiment, rType ):
        # type: (str, str, int) -> cdms.fvariable.FileVariable
        dataset = self.getDataset( experiment, rType )
        return dataset(varName)

    def getVariables( self, experiment, rType ):
        # type: (str, int) -> list[cdms.fvariable.FileVariable]
        dataset = self.getDataset( experiment, rType )
        varnames = [ vname for vname in dataset.listvariables() if not vname.endswith("_bnds") ]
        varnames.sort()
        return [ dataset(varName) for varName in varnames ]

class InputVarRec:
    def __init__(self, _varname, _level = None ):
        self.varName = _varname
        self.level = _level
        self.id = self.varName + "-" + str(self.level) if self.level else self.varName





