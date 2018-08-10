import zmq, traceback, time, logging, xml, cdms2, socket
from threading import Thread
from typing import List, Dict, Sequence
from cdms2.variable import DatasetVariable
from random import SystemRandom
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

    def message(self) -> str: return self._body;

class Message ( Response ):

    def __init__(self,  _clientId: str,  _responseId: str,  _message: str ):
        super(Message, self).__init__( "message", _clientId, _responseId )
        self._body = _message

    def toString(self) -> str: return "Message[" + self.id() + "]: " + self._body


class ErrorReport(Response):

    def __init__( self,  clientId: str,  _responseId: str,  _message: str ):
        super(ErrorReport, self).__init__( "error", clientId, _responseId )
        self._body = _message

    def toString(self) -> str: return "ErrorReport[" + self.id() + "]: " + self._body


class DataPacket(Response):

    def __init__( self,  clientId: str,  responseId: str,  header: str, data: bytes = b""  ):
        super(DataPacket, self).__init__( "data", clientId, responseId )
        self._body =  header
        self._data = data

    def hasData(self) -> bool:
        return len( self._data ) > 0

    def getTransferHeader(self) -> bytes:
        return bytes( self.clientId + ":" + self._body )

    def getHeaderString(self) -> str:
        return self._body

    def getTransferData(self) -> bytes:
        return bytes(self.clientId) + self._data

    def getRawData(self) -> bytes:
        return self._data

    def toString(self) -> str: return \
        "DataPacket[" + self._body + "]"


class Responder(Thread):

    def __init__( self,  _context: zmq.Context,  _client_address: str,  _response_port: int ):
        super(Responder, self).__init__()
        self.logger =  logging.getLogger()
        self.context: zmq.Context =  _context
        self.active: bool = True
        self.response_port = _response_port
        self.response_queue = queue.Queue()
        self.executing_jobs: dict[str,Response] = {}
        self.status_reports: dict[str,str] = {}
        self.clients: set[str] = set()
        self.client_address = _client_address

    def registerClient( self, client: str ):
        self.clients.add(client)

    def sendResponse( self, msg: Response  ):
        self.logger.info( "Post Message to response queue: " + str(msg) )
        self.response_queue.put( msg )

    def sendDataPacket( self, data: DataPacket ):
        self.logger.info( "Post DataPacket to response queue: " + str(data) )
        self.response_queue.put( data )

    def doSendResponse( self, socket: zmq.Socket,  r: Response ):
        if( r.rtype == "message" ):
            packaged_msg = self.doSendMessage( socket, r )
            dateTime =  datetime.datetime.now()
            self.logger.info( " Sent response: " + r.id() + " (" + dateTime.strftime("MM/dd HH:mm:ss") + "), content sample: " + packaged_msg.substring( 0, min( 300, packaged_msg.length() ) ) );
        elif( r.rtype == "data" ):
            self.doSendDataPacket( socket, r )
        elif( r.rtype == "error" ):
                self.doSendErrorReport( socket, r )
        else:
            self.logger.error( "Error, unrecognized response type: " + r.rtype )
            self.doSendErrorReport( socket, ErrorReport( r.clientId, r.responseId, "Error, unrecognized response type: " + r.rtype ) )

    def doSendMessage(self, socket: zmq.Socket, msg: Message):
        request_args = [ msg.id(), "response", msg.message() ]
        packaged_msg = "!".join( request_args )
        socket.send( bytes(packaged_msg) )
        return packaged_msg

    def doSendErrorReport( self, socket: zmq.Socket, msg: ErrorReport  ):
        request_args = [ msg.id(), "error", msg.message() ]
        packaged_msg = "!".join( request_args )
        socket.send( bytes(packaged_msg) )
        return packaged_msg

    def doSendDataPacket( self, socket: zmq.Socket, dataPacket: DataPacket ):
        socket.send( dataPacket.getTransferHeader() )
        if( dataPacket.hasData() ): socket.send( dataPacket.getTransferData() )
        self.logger.info( " Sent data packet " + dataPacket.id() + ", header: " + dataPacket.getHeaderString() )

    def setExeStatus( self, cId: str, rid: str, status: str ):
        self.status_reports[rid] = status
        try:
            if status.startswith("executing"):
                self.executing_jobs[rid] = Response( "executing", cId, rid )
            elif (status.startswith("error")) or (status.startswith("completed") ):
                del self.executing_jobs[rid]
        except Exception: pass

    def heartbeat( self, socket: zmq.Socket ):
        for client in self.clients:
            try:
                hb_msg = Message( str(client), "status", "heartbeat" )
                self.doSendMessage( socket, hb_msg )
            except Exception: pass

    def run( self ):
        pause_time = 100;
        heartbeat_interval = 20 * 1000;
        last_heartbeat_time = time.time()
        socket: zmq.Socket   = self.context.socket(zmq.PUB)
        try:
            socket.bind( "tcp://{}:{}".format( self.client_address, self.response_port ) )
            self.logger.info( " --> Bound response socket to client at {} on port: {}".format( self.client_address, self.response_port ) )
        except Exception as err:
            self.logger.error( "Error initializing response socket on port {}: {}".format( self.response_port, err ) )
        try:
            while self.active:
                try:
                    response: Response = self.response_queue.get(False)
                    self.doSendResponse(socket,response)
                except queue.Empty:
                    time.sleep(pause_time)
                    current_time = time.time()
                    if ( current_time - last_heartbeat_time) >= heartbeat_interval:
                        self.heartbeat( socket )
                        last_heartbeat_time = current_time

        except KeyboardInterrupt: pass

        self.close_connection( socket )

    def term( self ):
        self.logger.info("Terminating responder thread")
        self.active = False

    def close_connection( self, socket: zmq.Socket ):
        try:
            for response in self.executing_jobs.values():
                self.doSendErrorReport( socket, ErrorReport(response.clientId, response.responseId, "Job terminated by server shutdown.") );
            socket.close()
        except Exception: pass


class EDASPortal:

#    def sendErrorReport( taskSpec: Sequence[str],  err: Exception  ):
#        pass

    def __init__( self,  client_address: str, request_port: int, response_port: int ):
        self.logger =  logging.getLogger()
        try:
            self.request_port = request_port
            self.zmqContext: zmq.Context = zmq.Context()
            self.request_socket: zmq.Socket = self.zmqContext.socket(zmq.REP)
            self.responder = Responder( self.zmqContext, client_address, response_port)
            self.responder.setDaemon(True)
            self.responder.start()
            self.active = True
            self.initSocket( client_address, request_port )

        except Exception as err:
            self.logger.error( "\n-------------------------------\nEDAS Init error: {} -------------------------------\n".format( err ) )

    def initSocket(self, client_address, request_port):
        try:
            self.request_socket.bind( "tcp://{}:{}".format( client_address, request_port ) )
            self.logger.info( " --> Bound request socket to client at {} on port: {}".format( client_address, request_port ) )
        except Exception as err:
            self.logger.error( "Error initializing request socket on port {}: {}".format( request_port, err ) )


    def randomStr(self, length )-> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join( random.SystemRandom().choice( tokens ) for _ in range( length ) )

    def sendErrorReport( self, clientId: str, responseId: str, msg: str ):
        self.logger.info("-----> SendErrorReport[" + clientId +":" + responseId + "]" )
        self.responder.sendResponse( ErrorReport(clientId,responseId,msg) )


    def setExeStatus( self, clientId: str, rid: str, status: str ):
        self.responder.setExeStatus(clientId,rid,status)


    def sendArrayData( self, clientId: str, rid: str, origin: Sequence[int], shape: Sequence[int], data: bytes, metadata: Dict[str,str] ):
        self.logger.debug( "@@ Portal: Sending response data to client for rid {}, nbytes={}".format( rid, data.length ) )
        array_header_fields = [ "array", rid, self.ia2s(origin), self.ia2s(shape), self.m2s(metadata), "1" ]
        array_header = "|".join(array_header_fields)
        header_fields = [ rid, "array", array_header ]
        header = "!".join(header_fields)
        self.logger.debug("Sending header: " + header)
        self.responder.sendDataPacket( DataPacket( clientId, rid, header, data ) )


    def sendFile( self, clientId: str, jobId: str, name: str, filePath: str, sendData: bool ) -> str:
        self.logger.debug( "Portal: Sending file data to client for {}, filePath={}".format( name, filePath ) )
        file = open(filePath)
        file_header_fields = [ "array", jobId, name, file.name ]
        if not sendData: file_header_fields.append(filePath)
        file_header = "|".join( file_header_fields )
        header_fields = [ jobId,"file", file_header ]
        header = "!".join(header_fields)
        try:
            data =  bytes(file.read()) if sendData else None
            self.logger.debug(" ##sendDataPacket: clientId=" + clientId + " jobId=" + jobId + " name=" + name + " path=" + filePath );
            self.responder.sendDataPacket( DataPacket( clientId, jobId, header, data ) )
            self.logger.debug("Done sending file data packet: " + header)
        except Exception as ex:
            self.logger.info( "Error sending file : " + filePath + ": " + str(ex) )
        return file.name


    def execUtility( self, utilSpec: Sequence[str] ) -> Message: pass
    def execute( self, taskSpec: Sequence[str] ) -> Response: pass
    def shutdown( self ): pass
    def getCapabilities( self, utilSpec: Sequence[str] ) -> Message: pass
    def describeProcess( self, utilSpec: Sequence[str] ) -> Message: pass

    def sendResponseMessage( self, msg: Response ) -> str:
        request_args = [ msg.id(), msg.message() ]
        packaged_msg = "!".join( request_args )
        timeStamp =  datetime.datetime.now().strftime("MM/dd HH:mm:ss")
        self.logger.info( "@@ Sending response {} on request_socket @({}): {}".format( msg.responseId, timeStamp, msg.toString() ) )
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
            self.logger.info(  "Listening for requests on port: {}, host: {}".format( self.request_port, self.getHostInfo() ) )
            request_header = str( self.request_socket.recv(0) ).strip()
            parts = request_header.split("!")
            self.responder.registerClient( parts[0] )
            try:
                timeStamp = datetime.datetime.now().strftime("MM/dd HH:mm:ss")
                self.logger.info( "  ###  Processing {} request: {} @({})".format( parts[1], request_header, timeStamp) )
                if parts[1] == "execute":
                    self.sendResponseMessage( self.execute(parts) )
                elif parts[1] == "util":
                    self.sendResponseMessage( self.execUtility(parts));
                elif parts[1] == "quit" or parts[1] == "shutdown":
                    self.sendResponseMessage( Message(parts[0], "quit", "Terminating") )
                    self.logger.info("Received Shutdown Message")
                    exit(0)
                elif parts[1].lower() == "getcapabilities":
                    self.sendResponseMessage( self.getCapabilities(parts) )
                elif parts[1].lower() == "describeprocess":
                    self.sendResponseMessage( self.describeProcess(parts) )
                else:
                    msg = "Unknown request header type: " + parts[1]
                    self.logger.info(msg)
                    self.sendResponseMessage( Message(parts[0], "error", msg) )
            except Exception as ex:
                # clientId = elem( self.taskSpec, 0 )
                # runargs = self.getRunArgs( self.taskSpec )
                # jobId = runargs.getOrElse("jobId", self.randomIds.nextString)
                self.sendResponseMessage( Message( parts[0], "error", str(ex)) )

        self.logger.info( "EXIT EDASPortal");

    def term( self, msg ):
        self.logger.info( "!!EDAS Shutdown: " + msg )
        self.active = False
        self.logger.info( "QUIT PythonWorkerPortal")
        try: self.request_socket.close()
        except Exception: pass
        self.logger.info( "CLOSE request_socket")
        self.responder.term()
        self.logger.info( "TERM responder")
        self.shutdown()
        self.logger.info( "shutdown complete")

    def ia2s( self, array: Sequence[int] ) -> str:
        return str(array).strip("[]")

    def sa2s( self, array: Sequence[str] ) -> str:
        return ",".join(array)

    def m2s( self, metadata: Dict[str,str] ) -> str:
        items = [ ":".join(item) for item in metadata.items() ]
        return ";".join(items)
