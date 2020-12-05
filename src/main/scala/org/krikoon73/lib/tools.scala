package org.krikoon73.lib

import org.apache.log4j.LogManager

object LoggerHelper{
  def methodName() = Thread.currentThread().getStackTrace()(3).getMethodName
  val log = LogManager.getLogger("GradientDescent")
  val header = "Gradient Descent tests"
  def logInfo(msg: String= "")= if(msg.length > 0) log.info(String.format("(%s) - %s", methodName(), msg)) else log.info("")
  def logDebug(msg: String= "")= if(msg.length > 0) log.debug(String.format("(%s) - %s", methodName(), msg)) else log.debug("")
  def logWarn(msg: String= "")= if(msg.length > 0) log.warn(String.format("(%s) - %s", methodName(), msg)) else log.warn("")
  def logError(msg: String= "")= if(msg.length > 0) log.error(String.format("(%s) - %s", methodName(), msg)) else log.error("")
  def title = log.warn(header)
}
