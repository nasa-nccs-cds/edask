from typing import Dict, Any, Union

import zmq, traceback, time, logging, xml, cdms2, socket, defusedxml
from xml.etree.ElementTree import Element, ElementTree
from threading import Thread
from cdms2.variable import DatasetVariable
from random import SystemRandom
import random, string, os, queue, datetime, atexit
from edask.portal.base import EDASPortal, Message, Response
from typing import List, Dict, Sequence
from edask.workflow.module import edasOpManager
from edask.process.manager import ProcessManager
from enum import Enum

class EDASapp(EDASPortal):

    # class ExecutionCallback:
    #     def __init(self, _app: EDASapp ,_logger: Logger ):
    #         self.logger = _logger
    #         self.app = _app
    #
    #     def success( results: xml.Node ):
    #         metadata  = {}
    #         self.logger.info(" *** ExecutionCallback: jobId = ${jobId}, responseType = ${responseType} *** " )
    #         if responseType == "object":
    #             metadata  =   self.app.sendDirectResponse(response_syntax, clientId, jobId, results)
    #         elif responseType == "file":\
    #             metadata  =   self.app.sendFileResponse(response_syntax, clientId, jobId, results)
    #         self.app.setExeStatus(clientId, jobId, "completed|" + ( metadata.map { case (key,value) => key + ":" + value } mkString(",") ) )
    #
    #     def failure( msg: str ):
    #         self.logger.error( "ERROR CALLBACK ($jobId:$clientId): " + msg )
    #         self.setExeStatus( clientId, jobId, "error" )

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default

    # @staticmethod
    # def getConfiguration( parameter_file_path: String ): Map[String, String] = {
    #     if( parameter_file_path.isEmpty ) { Map.empty[String, String]  }
    #     else if( Files.exists( Paths.get(parameter_file_path) ) ) {
    #       val params: Iterator[Array[String]] = for ( line <- Source.fromFile(parameter_file_path).getLines() ) yield { line.split('=') }
    #       Map( params.filter( _.length > 1 ).map( a => ( a.head.trim, a.last.trim ) ).toSeq: _* )
    #     }
    #     else { throw new Exception( "Can't find parameter file: " + parameter_file_path) }
    #   }

    def __init__( self, client_address: str="127.0.0.1", request_port: int=4556, response_port: int=4557, appConfiguration: Dict[str,str]={} ):
        super( EDASapp, self ).__init__( client_address, request_port, response_port )
        self.processManager = ProcessManager( appConfiguration )
        self.process = "edas"
        atexit.register( self.term, "ShutdownHook Called" )

    def start( self ): self.run()

    def getCapabilities(self, utilSpec: Sequence[str] ) -> Message:
        capabilities = edasOpManager.getCapabilitiesStr()
        return Message( utilSpec[0], "capabilities", capabilities )


    def describeProcess(self, utilSpec: Sequence[str] ) -> Message:
        ( module, op ) = utilSpec[1].split(":")
        description = edasOpManager.describeProcess( module, op )
        return Message( utilSpec[0], "capabilities", description )


    def execUtility( self, utilSpec: Sequence[str] ) -> Message:
        return Message("","","")

    def getRunArgs( self, taskSpec: Sequence[str] )-> Dict[str,str]:
        runargs: Dict[str,str] = self.parseMap( taskSpec[4] )  if( len(taskSpec) > 4 ) else {}
        responseForm = str( runargs.get("responseform","wps") )
        if responseForm == "wps":
          responseType = runargs.get( "response", self.defaultResponseType(runargs) )
          rv = { k: str(v) for k, v in runargs.items() }
          rv["response" ] = responseType
          return rv
        else:
          responseToks = responseForm.split(':')
          new_runargs = { k: str(v) for k, v in runargs.items() }
          new_runargs["response" ] = responseToks[0]
          if( responseToks[0].lower() == "collection" and len(responseToks) > 1  ): new_runargs["cid"] = responseToks[-1]
          return new_runargs

    def parseMap( self, serialized_map: str )-> Dict[str,str]:
        return {}

    def defaultResponseType( self, runargs:  Dict[str, Any] )-> str:
         status = bool(str(runargs.get("status","false")))
         return "file" if status else "xml"

    def execute( self, taskSpec: Sequence[str] )-> Response:
        clientId = self.elem(taskSpec,0)
        runargs = self.getRunArgs( taskSpec )
        jobId = runargs.get( "jobId",self.randomStr(8) )
        process_name = self.elem(taskSpec,2)
        dataInputsSpec = self.elem(taskSpec,3)
        self.setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
        self.logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + str(runargs) )
        responseType = runargs.get("response","file")
        responseElem = ""
        return Message(clientId, jobId, responseElem )

        # 
        # 
        # try:
        #   (rid, responseElem) = processManager.executeProcess( process, Job(jobId, process_name, dataInputsSpec, runargs, 1f ), Some(executionCallback) )
        #   return Message(clientId, jobId, responseElem )
        # except Exception as err:
        #     self.logger.error( "Caught execution error: " + str(err) )
        #     self.logger.error( "\n" + err.getStackTrace().mkString("\n") )
        #     self.executionCallback.failure( str(err) )
        #     return ErrorReport( clientId, jobId, str(err) )

    # def sendErrorReport( self, clientId: str, responseId: str, exc: Exception ):
    #     err = WPSExceptionReport(exc)
    #     self.sendErrorReport( clientId, responseId, err.toXml() )
    #
    # def sendErrorReport( self, taskSpec: Sequence[str], exc: Exception ):
    #     clientId = taskSpec[0]
    #     runargs = self.getRunArgs( taskSpec )
    #     syntax = self.getResponseSyntax(runargs)
    #     err = WPSExceptionReport(exc)
    #     return self.sendErrorReport( clientId, "requestError", err.toXml() )


    def shutdown(self):
        self.processManager.term()

    def sendFileResponse( self, clientId: str, jobId: str, response: str  ) -> Dict[str,str]:
        return {}


    def sendDirectResponse( self, clientId: str, responseId: str, response: str ) -> Dict[str,str]:
        return {}



#         refs: list[Element] = response.findall( "data" )
#         val resultHref = refs.flatMap( _.attribute("href") ).find( _.nonEmpty ).map( _.text ) match {
#           case Some( href ) =>
#             val rid = href.split("[/]").last
#             logger.info( "\n\n     **** Found result Id: " + rid + " ****** \n\n")
#             processManager.getResultVariable("edas",rid) match {
#               case Some( resultVar ) =>
#                 val slice: CDRecord = resultVar.result.concatSlices.records.head
#                 slice.elements.foreach { case ( id, array ) =>
#                   sendArrayData( clientId, rid, array.origin, array.shape, array.toByteArray, resultVar.result.metadata  + ( "elem" -> id ) )  // + ("gridfile" -> gridfilename)
#                 }
#
#     //            var gridfilename = ""
#     //            resultVar.result.slices.foreach { case (key, data) =>
#     //              if( gridfilename.isEmpty ) {
#     //                val gridfilepath = data.metadata("gridfile")
#     //                gridfilename = sendFile( clientId, rid, "gridfile", gridfilepath, true )
#     //              }
#               case None =>
#                 logger.error( "Can't find result variable " + rid)
#                 sendErrorReport( response_format, clientId, rid, new Exception( "Can't find result variable " + rid + " in [ " + processManager.getResultVariables("edas").mkString(", ") + " ]") )
#             }
#           case None =>
#             logger.error( "Can't find result Id in direct response: " + response.toString() )
#             sendErrorReport( response_format, clientId, responseId, new Exception( "Can't find result Id in direct response: " + response.toString()  ) )
#         }
#         Map.empty[String,String]
#
#   def getNodeAttribute( node: xml.Node, attrId: String ): Option[String] = {
#     node.attribute( attrId ).flatMap( _.find( _.nonEmpty ).map( _.text ) )
#   }
#
#   def getNodeAttributes( node: xml.Node ): String = node.attributes.toString()
#
#   def sendFileResponse( response_format: ResponseSyntax.Value, clientId: String, jobId: String, response: xml.Node  ): Map[String,String] =  {
#     val refs: xml.NodeSeq = response \\ "data"
#     var result_files = new ArrayBuffer[String]()
#     for( node: xml.Node <- refs; hrefOpt = getNodeAttribute( node,"href"); fileOpt = getNodeAttribute( node,"files") ) {
#       if (hrefOpt.isDefined && fileOpt.isDefined) {
#         val sharedDataDir = appParameters( "wps.shared.data.dir" )
# //        val href = hrefOpt.get
# //        val rid = href.split("[/]").last
#         fileOpt.get.split(",").foreach( filepath => {
#           logger.info("     ****>> Found file node for jobId: " + jobId + ", clientId: " + clientId + ", sending File: " + filepath + " ****** ")
#           result_files += sendFile( clientId, jobId, "publish", filepath, sharedDataDir.isEmpty )
#         })
#       } else if (hrefOpt.isDefined && hrefOpt.get.startsWith("collection")) {
#         responder.sendResponse( new Message( clientId, jobId, "Created " + hrefOpt.get ) )
#       } else {
#         sendErrorReport( response_format, clientId, jobId, new Exception( "Can't find href or files in attributes: " + getNodeAttributes( node ) ) )
#       }
#     }
#     Map( "files" -> result_files.mkString(",") )
#
#
#
#
#


# //  def getResult( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
# //    val result: xml.Node = processManager.getResult( process, resultSpec(0),response_syntax )
# //    sendResponse( resultSpec(0), printer.format( result )  )
# //  }
# //
# //  def getResultStatus( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
# //    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0), response_syntax )
# //    sendResponse( resultSpec(0), printer.format( result )  )
# //  }


if __name__ == "__main__":
    server = EDASapp()