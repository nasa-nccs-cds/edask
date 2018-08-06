from typing import Dict, Any, Union

import zmq, traceback, time, logging, xml, cdms2, socket
from threading import Thread
from cdms2.variable import DatasetVariable
from random import SystemRandom
import random, string, os, queue, datetime, atexit
from .base import EDASPortal, Message, Response, ExecutionCallback
from enum import Enum

class EDASapp(EDASPortal):

    class ExecutionCallback:
        def __init(self, _app: EDASapp ,_logger: Logger ):
            self.logger = _logger
            self.app = _app

        def success( results: xml.Node ):
            metadata  = {}
            self.logger.info(" *** ExecutionCallback: jobId = ${jobId}, responseType = ${responseType} *** " )
            if responseType == "object":
                metadata  =   self.app.sendDirectResponse(response_syntax, clientId, jobId, results)
            elif responseType == "file":\
                metadata  =   self.app.sendFileResponse(response_syntax, clientId, jobId, results)
            self.app.setExeStatus(clientId, jobId, "completed|" + ( metadata.map { case (key,value) => key + ":" + value } mkString(",") ) )

        def failure( msg: str ):
            self.logger.error( "ERROR CALLBACK ($jobId:$clientId): " + msg )
            self.setExeStatus( clientId, jobId, "error" )

    @staticmethod
    def elem( array: list[str], index: int, default: str = "" )-> str:
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

    def __init__( self, client_address: str, request_port: int, response_port: int, appConfiguration: dict[str,str] ):
        super( EDASapp, self ).__init__( client_address, request_port, response_port )
        self.processManager = new ProcessManager( appConfiguration )
        self.process = "edas"
        atexit.register( self.term, "ShutdownHook Called" )

    def start( self, run_program: str = "" ):
        if not run_program: self.run()
        else: self.runAltProgram(run_program)


    def runAltProgram( self, run_program: str ):
        pass


    def execUtility( self, utilSpec: list[str] ) -> Message:
        Message("","","")

    def getRunArgs( self, taskSpec: list[str] )-> dict[str,str]:
        runargs: Dict[str,str] = wpsObjectParser.parseMap( taskSpec[4] )  if( len(taskSpec) > 4 ) else {}
        responseForm = runargs.getOrElse("responseform","wps").toString
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

    def defaultResponseType( runargs:  Dict[str, Any] )-> str:
         status = bool(str(runargs.get("status","false")))
         return "file" if status else "xml"

    def execute( self, taskSpec: list[str] )-> Response:
        clientId = self.elem(taskSpec,0)
        runargs = self.getRunArgs( taskSpec )
        jobId = runargs.get( "jobId",self.randomStr(8) )
        process_name = self.elem(taskSpec,2)
        dataInputsSpec = self.elem(taskSpec,3)
        self.setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
        self.logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + str(runargs) )
        response_syntax = self.getResponseSyntax(runargs)
        responseType = runargs.get("response","file")


        try {
          val (rid, responseElem) = processManager.executeProcess( process, Job(jobId, process_name, dataInputsSpec, runargs, 1f ), Some(executionCallback) )
          new Message(clientId, jobId, printer.format(responseElem))
        } catch  {
          case e: Throwable =>
            logger.error( "Caught execution error: " + e.getMessage )
            logger.error( "\n" + e.getStackTrace().mkString("\n") )
            executionCallback.failure( e.getMessage )
            new ErrorReport( clientId, jobId, e.getClass.getSimpleName + ": " + e.getMessage )

  def sendErrorReport( response_format: ResponseSyntax.Value, clientId: String, responseId: String, exc: Exception ): Unit = {
    val err = new WPSExceptionReport(exc)
    sendErrorReport( clientId, responseId, printer.format( err.toXml(response_format) ) )
  }

  def sendErrorReport( taskSpec: Array[String], exc: Exception ): Unit = {
    val clientId = taskSpec(0)
    val runargs = getRunArgs( taskSpec )
    val syntax = getResponseSyntax(runargs)
    val err = new WPSExceptionReport(exc)
    sendErrorReport( clientId, "requestError", printer.format( err.toXml(syntax) ) )
  }

  override def shutdown = processManager.term

  def sendDirectResponse( response_format: ResponseSyntax.Value, clientId: String, responseId: String, response: xml.Node ): Map[String,String] =  {
    val refs: xml.NodeSeq = response \\ "data"
    val resultHref = refs.flatMap( _.attribute("href") ).find( _.nonEmpty ).map( _.text ) match {
      case Some( href ) =>
        val rid = href.split("[/]").last
        logger.info( "\n\n     **** Found result Id: " + rid + " ****** \n\n")
        processManager.getResultVariable("edas",rid) match {
          case Some( resultVar ) =>
            val slice: CDRecord = resultVar.result.concatSlices.records.head
            slice.elements.foreach { case ( id, array ) =>
              sendArrayData( clientId, rid, array.origin, array.shape, array.toByteArray, resultVar.result.metadata  + ( "elem" -> id ) )  // + ("gridfile" -> gridfilename)
            }

//            var gridfilename = ""
//            resultVar.result.slices.foreach { case (key, data) =>
//              if( gridfilename.isEmpty ) {
//                val gridfilepath = data.metadata("gridfile")
//                gridfilename = sendFile( clientId, rid, "gridfile", gridfilepath, true )
//              }
          case None =>
            logger.error( "Can't find result variable " + rid)
            sendErrorReport( response_format, clientId, rid, new Exception( "Can't find result variable " + rid + " in [ " + processManager.getResultVariables("edas").mkString(", ") + " ]") )
        }
      case None =>
        logger.error( "Can't find result Id in direct response: " + response.toString() )
        sendErrorReport( response_format, clientId, responseId, new Exception( "Can't find result Id in direct response: " + response.toString()  ) )
    }
    Map.empty[String,String]
  }

  def getNodeAttribute( node: xml.Node, attrId: String ): Option[String] = {
    node.attribute( attrId ).flatMap( _.find( _.nonEmpty ).map( _.text ) )
  }

  def getNodeAttributes( node: xml.Node ): String = node.attributes.toString()

  def sendFileResponse( response_format: ResponseSyntax.Value, clientId: String, jobId: String, response: xml.Node  ): Map[String,String] =  {
    val refs: xml.NodeSeq = response \\ "data"
    var result_files = new ArrayBuffer[String]()
    for( node: xml.Node <- refs; hrefOpt = getNodeAttribute( node,"href"); fileOpt = getNodeAttribute( node,"files") ) {
      if (hrefOpt.isDefined && fileOpt.isDefined) {
        val sharedDataDir = appParameters( "wps.shared.data.dir" )
//        val href = hrefOpt.get
//        val rid = href.split("[/]").last
        fileOpt.get.split(",").foreach( filepath => {
          logger.info("     ****>> Found file node for jobId: " + jobId + ", clientId: " + clientId + ", sending File: " + filepath + " ****** ")
          result_files += sendFile( clientId, jobId, "publish", filepath, sharedDataDir.isEmpty )
        })
      } else if (hrefOpt.isDefined && hrefOpt.get.startsWith("collection")) {
        responder.sendResponse( new Message( clientId, jobId, "Created " + hrefOpt.get ) )
      } else {
        sendErrorReport( response_format, clientId, jobId, new Exception( "Can't find href or files in attributes: " + getNodeAttributes( node ) ) )
      }
    }
    Map( "files" -> result_files.mkString(",") )


  override def getCapabilities(utilSpec: Array[String]): Message = {
    val runargs: Map[String,String] = getRunArgs( utilSpec )
    logger.info(s"Processing getCapabilities request with args ${runargs.toList.mkString(",")}" )
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,2), runargs )
    new Message( utilSpec(0), "capabilities", printer.format( result ) )


  override def describeProcess(procSpec: Array[String]): Message = {
    val runargs: Map[String,String] = getRunArgs( procSpec )
    logger.info(s"Processing describeProcess request with args ${runargs.toList.mkString(",")}" )
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,2), runargs )
    new Message( procSpec(0), "preocesses", printer.format( result ) )




# //  def getResult( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
# //    val result: xml.Node = processManager.getResult( process, resultSpec(0),response_syntax )
# //    sendResponse( resultSpec(0), printer.format( result )  )
# //  }
# //
# //  def getResultStatus( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
# //    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0), response_syntax )
# //    sendResponse( resultSpec(0), printer.format( result )  )
# //  }
