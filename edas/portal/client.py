import zmq, traceback, time, logging, xml
from threading import Thread
from typing import Sequence, List, Dict, Mapping, Optional
from edas.util.logging import EDASLogger
from edas.process.task import UID
import xarray as xa
import random, string, os
from enum import Enum
MB = 1024 * 1024

def s2b( s: str ):
    return bytearray( s, 'utf-8'  )

def b2s( b: bytearray ):
    return b.decode( 'utf-8'  )

class ConnectionMode():
    BIND = 1
    CONNECT = 2
    DefaultPort = 4336

    @classmethod
    def bindSocket( cls, socket: zmq.Socket, server_address: str, port: int ):
        test_port = port if( port > 0 ) else cls.DefaultPort
        while( True ):
            try:
                socket.bind( "tcp://{0}:{1}".format(server_address,test_port) )
                return test_port
            except Exception as err:
                test_port = test_port + 1

    @classmethod
    def connectSocket( cls, socket: zmq.Socket, host: str, port: int ):
        socket.connect("tcp://{0}:{1}".format( host, port ) )
        return port

class MessageState(Enum):
    ARRAY = 0
    FILE = 1
    RESULT = 2

class ResponseManager(Thread):

    def __init__(self, context: zmq.Context, clientId: str, host: str, port: int, **kwargs ):
        from edas.config import EdaskEnv
        Thread.__init__(self)
        self.context = context
        self.logger = EDASLogger.getLogger()
        self.host = host
        self.port = port
        self.clientId = clientId
        self.active = True
        self.mstate = MessageState.RESULT
        self.setName('EDAS Response Thread')
        self.cached_results = {}
        self.cached_arrays = {}
        self.filePaths = {}
        self.diag = bool(kwargs.get("diag",False))
        self.setDaemon(True)
        self.cacheDir = EdaskEnv.CONFIG_DIR
        self.log("Created RM, cache dir = " + self.cacheDir )

    def cacheResult(self, id: str, result: str ):
        self.logger.info( "Caching result array: " + id )
        self.getResults(id).append(result)

    def getResults(self, id: str ) -> List[str]:
        return self.cached_results.setdefault(id,[])

    def cacheArray(self, id: str, array ):
        print( "Caching array: " + id )
        self.getArrays(id).append(array)

    def getArrays(self, id: str ):
        return self.cached_arrays.setdefault(id,[])

    def run(self):
        response_socket = None
        try:
            self.log("Run RM thread")
            response_socket: zmq.Socket = self.context.socket( zmq.PULL )
            response_port = ConnectionMode.connectSocket( response_socket, self.host, self.port )
#            response_socket.subscribe( self.clientId )
            self.log("Connected response socket on port {} with subscription (client) id: '{}'".format( response_port, self.clientId ) )
            while( self.active ):
                self.processNextResponse( response_socket )

        except Exception: pass
        finally:
            if response_socket: response_socket.close()

    def term(self):
        self.log("Terminate RM thread")
        if self.active:
            self.active = False

    def popResponse(self) -> Optional[str]:
        if( len( self.cached_results ) == 0 ):
            return None
        else:
            return self.cached_results.pop()

    def getMessageField(self, header, index ) -> str:
        toks = header.split('|')
        return toks[index]

    def log(self, msg: str, maxPrintLen = 300 ):
        self.logger.info( "[RM] " + msg )
        print(  "[RM] " + msg[0:maxPrintLen] )


    #    def getResponse(self, key, default = None ):
    #       return self.cached_results.get( key, default )

    def getItem(self, str_array: Sequence[str], itemIndex: int, default_val="NULL" ) -> str:
        try: return str_array[itemIndex]
        except Exception as err: return default_val

    def processNextResponse(self, socket: zmq.Socket ):
        try:
            self.log("Awaiting responses" )
            response = socket.recv()
            toks: List[bytearray] = response.split( s2b('!') )
            rId = b2s( toks[0] )
            type = b2s( toks[1] )
            msg = b2s( toks[2] )
            self.log("Received response, rid: " + rId + ", type: " + type )
            if type == "array":
                self.log( "\n\n #### Received array " + rId + ": " + msg )
                data = socket.recv()
                array = data # npArray.createInput(msg,data)
                self.logger.info("Received array: {0}".format(rId))
                self.cacheArray( rId, array )
            elif type == "file":
                self.log("\n\n #### Received file " + rId + ": " + msg)
                filePath = self.saveFile( msg, socket )
                self.filePaths[rId] = filePath
                self.log("Saved file '{0}' for rid {1}".format(filePath,rId))
                if self.diag: self.log( str( xa.open_dataset(filePath) ) ) # .to_netcdf()
            elif type == "error":
                self.log(  "\n\n #### ERROR REPORT " + rId + ": " + msg )
                print (" *** Execution Error Report: " + msg)
                self.cacheResult( rId, msg )
            elif type == "response":
                if rId == "status":
                    print (" *** Execution Status Report: " + msg)
                else:
                    self.log(  " Caching response message " + rId  + ", sample: " + msg[0:300] )
                    self.cacheResult( rId, msg )
            else:
                self.log(" #### EDASPortal.ResponseThread-> Received unrecognized message type: {0}".format(type))

        except Exception as err:
            self.log( "EDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ), 1000 )

    def getFileCacheDir( self, role: str ) -> str:
        filePath = os.path.join( self.cacheDir, "transfer", role )
        if not os.path.exists(filePath): os.makedirs(filePath)
        self.log(" ***->> getFileCacheDir = {0}".format(filePath) )
        return filePath

    def saveFile(self, header: str, socket: zmq.Socket ):
        header_toks = header.split('|')
        id = header_toks[1]
        role = header_toks[2]
        fileName = os.path.basename(header_toks[3])
        data = socket.recv()
        filePath = os.path.join( self.getFileCacheDir(role), fileName )
        self.log(" %%%% filePath = {0}".format(filePath) )
        with open( filePath, mode='wb') as file:
            file.write( data )
            self.log(" ***->> Saving File, path = {0}".format(filePath) )
        return filePath


    def getResponses( self, rId: str, wait: bool =True ):
        self.log(  "Waiting for a response from the server... " )
        try :
            while( True ):
                results = self.getResults(rId)
                if( (len(results) > 0) or not wait): return results
                else:
                    print (".")
                    time.sleep(0.25)
        except KeyboardInterrupt:
            self.log("Terminating wait for response")
            return []

    # def getResponseVariables(self, rId: str, wait=True):
    #     """  :rtype: list[DatasetVariable] """
    #     responses = self.getResponses( rId, wait )
    #     gridFileDir = self.getFileCacheDir("gridfile")
    #     vars = []
    #     for response in responses:
    #         self.log( "Processing response node: " + response )
    #         e = defusedxml.ElementTree.fromstring( response )
    #         for data_node in e.iter('data'):
    #             resultUri = data_node.get("href","")
    #             if resultUri:
    #                 self.log( "Processing response: " + resultUri )
    #                 resultId = resultUri.split("/")[-1]
    #                 result_arrays = self.cached_arrays.get( resultId, [] )
    #                 if result_arrays:
    #                     for result_array in result_arrays:
    #                         gridFilePath = os.path.join(gridFileDir, result_array.gridFile )
    #                         vars.append( result_array.getVariable( gridFilePath ) )
    #                 else:
    #                     from cdms2.dataset import Dataset
    #                     resultFilePath = self.filePaths.get( rId, data_node.get("file", "") )
    #                     self.log( "Processing file: " + resultFilePath )
    #                     if resultFilePath:
    #                         dset = cdms2.open( resultFilePath ); """:type : Dataset """
    #                         for fvar in dset.variables:
    #                             vars.append( dset(fvar) )
    #                     else:
    #                         self.logger.error( "Empty response node: " + str(data_node) )
    #             else :
    #                 self.logger.error( "Empty response node: " + str(data_node) )
    #
    #         return vars

class EDASPortalClient:

    def __init__( self, host: str="127.0.0.1", request_port: int=4556, response_port: int=4557, **kwargs ):
        try:
            self.active = True
            self.app_host = host
            self.application_thread = None
            self.clientID = UID.randomId(6)
            self.logger =  EDASLogger.getLogger()
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.REQ)

            # if( connectionMode == ConnectionMode.BIND ):
            #     self.request_port = ConnectionMode.bindSocket( self.request_socket, self.app_host, request_port )
            #     self.response_port = ConnectionMode.bindSocket( self.response_socket, self.app_host, response_port )
            #     self.logger.info( "Binding request socket to port: {0} (Requested {1})".format( self.request_port, request_port ) )
            #     self.logger.info( "Binding response socket to port: {0} (Requested {1}".format( self.response_port, response_port ) )
            # else:

            self.request_port = ConnectionMode.connectSocket(self.request_socket, self.app_host, request_port)
            self.log("[1]Connected request socket to server {0} on port: {1}".format( self.app_host, self.request_port ) )

            self.response_manager = ResponseManager(self.context, self.clientID, host, response_port, **kwargs)
            self.response_manager.start()


        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            print (err_msg)
            self.shutdown()

    def log(self, msg: str ):
        self.logger.info( "[P] " + msg )
        print  (msg)

    def __del__(self):
        print(  " Portal client being deleted " )
        self.shutdown()

    # def start_EDAS(self):  # Stage the EDAS app using the "{EDAS_HOME}>> sbt stage" command.
    #     self.application_thread = AppThread( self.app_host, self.request_port, self.response_port )
    #     self.application_thread.start()

    def createResponseManager(self) -> ResponseManager:
        return self.response_manager

    def shutdown(self):
        if self.active:
            print(  " ############################## Disconnect Portal Client from Server & shutdown Client ##############################"  )
            self.active = False
            try: self.request_socket.close()
            except Exception: pass
            if( self.application_thread ):
                response = self.sendMessage("shutdown")
                self.application_thread.term()
                self.application_thread = None
            if not (self.response_manager is None):
                self.response_manager.term()
                self.response_manager = None

    def sendMessage(self, type: str, mDataList=None):
        if mDataList is None:
            mDataList = [""]
        msgStrs = [ str(mData).replace("'",'"') for mData in mDataList ]
        self.log( "Sending {0} request {1} on port {2}.".format( type, msgStrs, self.request_port )  )
        try:
            message = "!".join( [self.clientID,type] + msgStrs )
            self.request_socket.send_string( message )
            response = self.request_socket.recv()
        except zmq.error.ZMQError as err:
            self.logger.error( "Error sending message {0} on request socket: {1}".format( message, str(err) ) )
            response = str(err)
        return response

    def waitUntilDone(self):
        self.response_manager.join()

class AppThread(Thread):
    def __init__(self, host, request_port, response_port):
        Thread.__init__(self)
        self.logger = EDASLogger.getLogger()
        self._response_port = response_port
        self._request_port = request_port
        self._host = host
        self.process = None
        self.setDaemon(True)

    def run(self):
        import subprocess, shlex, os
        from psutil import virtual_memory
        try:
            mem = virtual_memory()
            total_ram = mem.total / MB
            EDAS_DRIVER_MEM = os.environ.get( 'EDAS_DRIVER_MEM', str( total_ram - 1000 ) + 'M' )
            edas_startup = "" # ""edas connect {0} {1} -J-Xmx{2} -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( self._request_port, self._response_port, EDAS_DRIVER_MEM )
            self.process = subprocess.Popen(shlex.split(edas_startup))
            print ("Staring EDAS with command: {0}, total RAM: {1}\n".format( edas_startup, mem.total ))
            self.process.wait()
        except KeyboardInterrupt as ex:
            print ("  ----------------- EDAS TERM ----------------- ")
            self.process.kill()

    def term(self):
        if not (self.process is None):
            self.process.terminate()

    def block(self):
        self.process.wait()

if __name__ == "__main__":
    client = EDASPortalClient()
