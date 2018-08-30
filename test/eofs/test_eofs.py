import cdms2 as cdms
import  math, cdutil, time, os
from edask.eofs.solver import EOFSolver
from edask.eofs.pcProject import InputVarRec

#------------------------------ SET PARAMETERS   ------------------------------
pname = "20CRv2c"
#pname = "MERRA2"
project = pname + "_EOFs"
variables = [ InputVarRec("ts")] # , InputVarRec("zg",80000) , InputVarRec("zg",50000), InputVarRec("zg",25000)
outDir = os.path.expanduser("~/results/")
nModes = 64
plotResults = True
plotComparison = False

if pname.lower() == "merra2":
    start_year = 1980
    end_year = 2015
elif pname.lower() == "20crv2c":
    start_year = 1851
    end_year = 2012

for var in variables:
    if pname.lower() == "merra2": data_path = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/Reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/' + var.varName + '.ncml'
    elif pname.lower() == "20crv2c": data_path = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/20CRv2c/mon/atmos/' + var.varName + '.ncml'
    else: raise Exception( "Unknown project name: " + pname )
    start_time = cdtime.comptime(start_year)
    end_time = cdtime.comptime(end_year)

    #------------------------------ READ DATA ------------------------------

    read_start = time.time()
    print( "Computing Eofs for variable " + var.varName + ", level = " + str(var.level) )

    if var.level:
        experiment = project + '_'+str(start_year)+'-'+str(end_year) + '_M' + str(nModes) + "_" + var.varName + "-" + str(var.level)
        f = cdms.open(data_path)
        variable = f( var.varName, latitude=(-80,80), level=(var.level,var.level), time=(start_time,end_time) )  # type: cdms.tvariable.TransientVariable
    else:
        experiment = project + '_'+str(start_year)+'-'+str(end_year) + '_M' + str(nModes) + "_" + var.varName
        f = cdms.open(data_path)
        variable = f( var.varName, latitude=(-80,80), time=(start_time,end_time) )  # type: cdms.tvariable.TransientVariable


    print( "Completed data read in " + str(time.time()-read_start) + " sec " )

    #------------------------------ COMPUTE EOFS  ------------------------------

    solver = EOFSolver( project, experiment, outDir )
    solver.compute( variable, nModes, detrend=True, scale=True )
    print( "Completed computing Eofs for variable " + var.varName + ", level = " + str( var.level ) )

    #------------------------------ PLOT RESULTS   ------------------------------

    if plotResults:
        solver.plotEOFs( 5 )    # Change MPL to VCS for vcs plots (thomas projection)
        solver.plotPCs( 5 )

    if plotComparison:
        solver.plotPCComparison(4)


    f.close()

print( "DONE" )





