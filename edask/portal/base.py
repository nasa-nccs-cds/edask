import zmq, traceback, time, logging, xml, cdms2, socket
from typing import List, Dict, Sequence, Set
from edask.util.logging import EDASLogger
import random, string, os, queue, datetime
from enum import Enum
MB = 1024 * 1024

# class ExecutionCallback:
#   def success( results: xml.Node  ): pass
#   def failure( msg: str ): pass

class Response:

    def __init__(self, _rtype: str, _clientId: str, _responseId: str ):
        self.clientId = _clientId
        self.responseId = _responseId
        self.rtype = _rtype
        self._body = None

    def id(self) -> str:
        return self.clientId + ":" + self.responseId

    def message(self) -> str: return self._body.strip()

    def __str__(self) -> str: return self.__class__.__name__ + "[" + self.id() + "]: " + str(self._body)

class Message ( Response ):

    def __init__(self,  _clientId: str,  _responseId: str,  _message: str ):
        super(Message, self).__init__( "message", _clientId, _responseId )
        self._body = _message

class ErrorReport(Response):

    def __init__( self,  clientId: str,  _responseId: str,  _message: str ):
        super(ErrorReport, self).__init__( "error", clientId, _responseId )
        self._body = _message


class DataPacket(Response):

    def __init__( self,  clientId: str,  responseId: str,  header: str, data: bytes = b""  ):
        super(DataPacket, self).__init__( "data", clientId, responseId )
        self._body =  header
        self._data = data

    def hasData(self) -> bool:
        return len( self._data ) > 0

    def getTransferHeader(self) -> bytes:
        return bytearray( self.clientId + ":" + self._body, 'utf-8' )

    def getHeaderString(self) -> str:
        return self.clientId + ":" + self._body

    def getTransferData(self) -> bytes:
        return bytearray( self.clientId, 'utf-8' ) + self._data

    def getRawData(self) -> bytes:
        return self._data

    def toString(self) -> str: return \
        "DataPacket[" + self._body + "]"


class Responder:

    def __init__( self,  _context: zmq.Context,  _client_address: str,  _response_port: int ):
        super(Responder, self).__init__()
        self.logger =  EDASLogger.getLogger()
        self.context: zmq.Context =  _context
        self.response_port = _response_port
        self.executing_jobs: Dict[str,Response] = {}
        self.status_reports: Dict[str,str] = {}
        self.clients: Set[str] = set()
        self.client_address = _client_address
        self.initSocket()

    def registerClient( self, client: str ):
        self.clients.add(client)

    def sendResponse( self, msg: Response ):
        self.logger.info( "@@R: Post Message to response queue: " + str(msg) )
        self.doSendResponse(msg)

    def sendDataPacket( self, data: DataPacket ):
        self.logger.info( "@@R: Posting DataPacket to response queue: " + str(data) )
        self.doSendResponse( data )
        self.logger.info("@@R: POST COMPLETE ")

    def doSendResponse( self,  r: Response ):
        if( r.rtype == "message" ):
            packaged_msg: str = self.doSendMessage( r )
            dateTime =  datetime.datetime.now()
            self.logger.info( "@@R: Sent response: " + r.id() + " (" + dateTime.strftime("MM/dd HH:mm:ss") + "), content sample: " + packaged_msg.substring( 0, min( 300, len(packaged_msg) ) ) );
        elif( r.rtype == "data" ):
            self.doSendDataPacket( r )
        elif( r.rtype == "error" ):
                self.doSendErrorReport( r )
        else:
            self.logger.error( "@@R: Error, unrecognized response type: " + r.rtype )
            self.doSendErrorReport( ErrorReport( r.clientId, r.responseId, "Error, unrecognized response type: " + r.rtype ) )

    def doSendMessage(self, msg: Response) -> str:
        self.logger.info("@@R: Sending MESSAGE: " + str(msg.message()))
        request_args = [ msg.id(), "response", msg.message() ]
        packaged_msg = "!".join( request_args )
        self.socket.send( bytearray( packaged_msg, 'utf-8' ) )
        return packaged_msg

    def doSendErrorReport( self, msg: Response  ):
        self.logger.info("@@R: Sending ERROR report: " + str(msg.message()))
        request_args = [ msg.id(), "error", msg.message() ]
        packaged_msg = "!".join( request_args )
        self.socket.send( bytearray( packaged_msg, 'utf-8' )  )
        return packaged_msg

    def doSendDataPacket( self, dataPacket: DataPacket ):
        header = dataPacket.getTransferHeader()
        self.socket.send( header )
        self.logger.info("@@R: Sent data header for " + dataPacket.id() + ": " + dataPacket.getHeaderString())
        if( dataPacket.hasData() ):
            bdata: bytes = dataPacket.getRawData()
            self.socket.send( bdata )
            self.logger.info("@@R: Sent data packet for " + dataPacket.id() + ", data Size: " + str(len(bdata)) )
        else:
            self.logger.info( "@@R: Sent data header only for " + dataPacket.id() + "---> NO DATA!" )

    def setExeStatus( self, cId: str, rid: str, status: str ):
        self.status_reports[rid] = status
        try:
            if status.startswith("executing"):
                self.executing_jobs[rid] = Response( "executing", cId, rid )
            elif (status.startswith("error")) or (status.startswith("completed") ):
                del self.executing_jobs[rid]
        except Exception: pass

    def heartbeat( self ):
        for client in self.clients:
            try:
                hb_msg = Message( str(client), "status", "heartbeat" )
                self.doSendMessage( hb_msg )
            except Exception: pass

    def initSocket(self):
        self.socket: zmq.Socket   = self.context.socket(zmq.PUSH)
        try:
            self.socket.bind( "tcp://{}:{}".format( self.client_address, self.response_port ) )
            self.logger.info( "@@R: --> Bound response socket to client at {} on port: {}".format( self.client_address, self.response_port ) )
        except Exception as err:
            self.logger.error( "@@R: Error initializing response socket on port {}: {}".format( self.response_port, err ) )

    def close_connection( self ):
        try:
            for response in self.executing_jobs.values():
                self.doSendErrorReport( self.socket, ErrorReport(response.clientId, response.responseId, "Job terminated by server shutdown.") );
            self.socket.close()
        except Exception: pass


class EDASPortal:

    def __init__( self,  client_address: str, request_port: int, response_port: int ):
        self.logger =  EDASLogger.getLogger()
        self.active = True
        try:
            self.request_port = request_port
            self.zmqContext: zmq.Context = zmq.Context()
            self.request_socket: zmq.Socket = self.zmqContext.socket(zmq.REP)
            self.responder = Responder( self.zmqContext, client_address, response_port)
            self.handlers = {}
            self.initSocket( client_address, request_port )

        except Exception as err:
            self.logger.error( "@@Portal:  ------------------------------- EDAS Init error: {} ------------------------------- ".format( err ) )

    def initSocket(self, client_address, request_port):
        try:
            self.request_socket.bind( "tcp://{}:{}".format( client_address, request_port ) )
            self.logger.info( "@@Portal --> Bound request socket to client at {} on port: {}".format( client_address, request_port ) )
        except Exception as err:
            self.logger.error( "@@Portal: Error initializing request socket on port {}: {}".format( request_port, err ) )

    def sendErrorReport( self, clientId: str, responseId: str, msg: str ):
        self.logger.info("@@Portal-----> SendErrorReport[" + clientId +":" + responseId + "]" )
        self.responder.sendResponse( ErrorReport(clientId,responseId,msg) )

    def addHandler(self, clientId, jobId, handler ):
        self.handlers[ clientId + "-" + jobId ] = handler
        return handler

    def removeHandler(self, clientId, jobId ):
        handlerId = clientId + "-" + jobId
        try:
            del self.handlers[ handlerId ]
        except:
            self.logger.error( "Error removing handler: " + handlerId + ", existing handlers = " + str(self.handlers.keys()))

    def setExeStatus( self, clientId: str, rid: str, status: str ):
        self.responder.setExeStatus(clientId,rid,status)

    def sendArrayData( self, clientId: str, rid: str, origin: Sequence[int], shape: Sequence[int], data: bytes, metadata: Dict[str,str] ):
        self.logger.debug( "@@Portal: Sending response data to client for rid {}, nbytes={}".format( rid, len(data) ) )
        array_header_fields = [ "array", rid, self.ia2s(origin), self.ia2s(shape), self.m2s(metadata), "1" ]
        array_header = "|".join(array_header_fields)
        header_fields = [ rid, "array", array_header ]
        header = "!".join(header_fields)
        self.logger.debug("Sending header: " + header)
        self.responder.sendDataPacket( DataPacket( clientId, rid, header, data ) )


    def sendFile( self, clientId: str, jobId: str, name: str, filePath: str, sendData: bool ) -> str:
        self.logger.debug( "@@Portal: Sending file data to client for {}, filePath={}".format( name, filePath ) )
        with open(filePath, mode='rb') as file:
            file_header_fields = [ "array", jobId, name, os.path.basename(filePath) ]
            if not sendData: file_header_fields.append(filePath)
            file_header = "|".join( file_header_fields )
            header_fields = [ jobId,"file", file_header ]
            header = "!".join(header_fields)
            try:
                data =  bytes(file.read()) if sendData else None
                self.logger.debug("@@Portal ##sendDataPacket: clientId=" + clientId + " jobId=" + jobId + " name=" + name + " path=" + filePath )
                self.responder.sendDataPacket( DataPacket( clientId, jobId, header, data ) )
                self.logger.debug("@@Portal Done sending file data packet: " + header)
            except Exception as ex:
                self.logger.info( "@@Portal Error sending file : " + filePath + ": " + str(ex) )
                traceback.print_exc()
            return file.name


    def execUtility( self, utilSpec: Sequence[str] ) -> Message: pass
    def execute( self, taskSpec: Sequence[str] ) -> Response: pass
    def shutdown( self ): pass
    def getCapabilities( self, type: str ) -> Message: pass
    def describeProcess( self, utilSpec: Sequence[str] ) -> Message: pass

    def sendResponseMessage( self, msg: Response ) -> str:
        request_args = [ msg.id(), msg.message() ]
        packaged_msg = "!".join( request_args )
        timeStamp =  datetime.datetime.now().strftime("MM/dd HH:mm:ss")
        self.logger.info( "@@Portal: Sending response {} on request_socket @({}): {}".format( msg.responseId, timeStamp, str(msg) ) )
        self.request_socket.send_string( packaged_msg )
        return packaged_msg


    # public static String getCurrentStackTrace() {
    #     try{ throw new Exception("Current"); } catch(Exception ex)  {
    #         Writer result = new StringWriter();
    #         PrintWriter printWriter = new PrintWriter(result);
    #         ex.printStackTrace(printWriter);
    #         return result.toString();
    #     }
    # }

    def getHostInfo(self) -> str:
        try:
            hostname = socket.gethostname()
            address = socket.gethostbyname(hostname)
            return  "{} ({})".format( hostname, address )
        except Exception as e:
            return "UNKNOWN"

    def run(self):
        while self.active:
            self.logger.info(  "@@Portal:Listening for requests on port: {}, host: {}".format( self.request_port, self.getHostInfo() ) )
            request_header = str( self.request_socket.recv(0) ).strip()
            parts = request_header.split("!")
            self.responder.registerClient( parts[0] )
            try:
                timeStamp = datetime.datetime.now().strftime("MM/dd HH:mm:ss")
                self.logger.info( "@@Portal:  ###  Processing {} request: {} @({})".format( parts[1], request_header, timeStamp) )
                if parts[1] == "execute":
                    self.sendResponseMessage( self.execute(parts) )
                elif parts[1] == "util":
                    self.sendResponseMessage( self.execUtility(parts));
                elif parts[1] == "quit" or parts[1] == "shutdown":
                    self.sendResponseMessage( Message(parts[0], "quit", "Terminating") )
                    self.logger.info("@@Portal: Received Shutdown Message")
                    exit(0)
                elif parts[1].lower() == "getcapabilities":
                    type = parts[1] if len(parts) > 1 else "kernels"
                    self.sendResponseMessage( self.getCapabilities(type) )
                elif parts[1].lower() == "describeprocess":
                    self.sendResponseMessage( self.describeProcess(parts) )
                else:
                    msg = "@@Portal: Unknown request header type: " + parts[1]
                    self.logger.info(msg)
                    self.sendResponseMessage( Message(parts[0], "error", msg) )
            except Exception as ex:
                # clientId = elem( self.taskSpec, 0 )
                # runargs = self.getRunArgs( self.taskSpec )
                # jobId = runargs.getOrElse("jobId", self.randomIds.nextString)
                self.logger.error( "@@Portal: Execution error: " + str(ex) )
                traceback.print_exc()
                self.sendResponseMessage( Message( parts[0], "error", str(ex)) )

        self.logger.info( "@@Portal: EXIT EDASPortal");

    def term( self, msg ):
        self.logger.info( "@@Portal: !!EDAS Shutdown: " + msg )
        self.active = False
        self.logger.info( "@@Portal: QUIT PythonWorkerPortal")
        try: self.request_socket.close()
        except Exception: pass
        self.logger.info( "@@Portal: CLOSE request_socket")
        self.responder.close_connection()
        self.logger.info( "@@Portal: TERM responder")
        self.shutdown()
        self.logger.info( "@@Portal: shutdown complete")

    def ia2s( self, array: Sequence[int] ) -> str:
        return str(array).strip("[]")

    def sa2s( self, array: Sequence[str] ) -> str:
        return ",".join(array)

    def m2s( self, metadata: Dict[str,str] ) -> str:
        items = [ ":".join(item) for item in metadata.items() ]
        return ";".join(items)
