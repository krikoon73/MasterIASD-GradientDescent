import java.io.File

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.{Level, Logger}

import scopt.OptionParser

import org.krikoon73.lib.BaseGradient
import org.krikoon73.lib.LoggerHelper

case class CommandLineArgs (
    AppName: String = "", 
    Master : String = "local[*]",
    stepsize: Double = 0.01,
    iterations: Int = 10,
    sizefeat: Int = 2
)

object GradientDescent {
    def main(args: Array[String]): Unit = {

        val parser = new OptionParser[CommandLineArgs]("GradientDescent") {
            head("Gradient Descent example","1.0")
            opt[String]('a',"appname")
                .required().valueName("<SPARK Application Name>")
                .action((x,a) => a.copy(AppName = x))
                .text("Setting SPARK Application Name is required")
            opt[String]('m',"master")
                .required().valueName("<Cluster or Local>")
                .action((x,m) => m.copy(Master = x))
                .text("Setting local or cluster execution is required")
            opt[Double]('s',"stepsize")
                .required().valueName("<Gradient Descent step>")
                .action((x,s) => s.copy(stepsize = x))
                .text("Setting gradient descent step size is required")
            opt[Int]('i',"iterations")
                .required().valueName("<Gradient Descent iteration>")
                .action((x,i) => i.copy(iterations = x))
                .text("Setting gradient descent iteration is required")
            opt[Int]('f',"features")
                .required().valueName("<Gradient Descent features>")
                .action((x,i) => i.copy(sizefeat = x))
                .text("Setting gradient descent features is required")
        }

        def time[R](block: => (String, R)): R = {
            val t0 = System.currentTimeMillis()
            val result = block._2
            val t1 = System.currentTimeMillis()
            //println(block._1 + " took Elapsed time of " + (t1 - t0) + " Millis")
            LoggerHelper.logWarn(s"${block._1} took Elapsed time of ${t1-t0} ms")
            result
        }

        parser.parse(args, CommandLineArgs()) match {
            case Some(config) => {
                //val conf = new SparkConf().setAppName("TestGradientDescent").setMaster("local[*]")
                val conf = new SparkConf().setAppName(config.AppName).setMaster(config.Master)
                val sc = new SparkContext(conf)
                sc.setLogLevel("WARN")

                LoggerHelper.logWarn("STARTING PROGRAM")
                val X = (1.0 to 10000).by(2).toArray
                val dtrain = sc.parallelize(X.map(x=>(5*x+2,Array(x,1.0))))
                
                val loading = time("Loading dtrain",dtrain.count())
                LoggerHelper.logWarn(s"Number of elements for dtrain : ${loading}")
                LoggerHelper.logWarn(s"Number of interations : ${config.iterations}")
                //LoggerHelper.logWarn(s"Result : ${s.time(BaseGradient(dtrain,0.00000000001,10000,2).foreach(println))}")
                val baseResult = time("Simple Gradient Descent", BaseGradient(dtrain,config.stepsize,config.iterations,config.sizefeat))
                //LoggerHelper.logWarn(s"Result : ${BaseGradient(dtrain,config.stepsize,config.iterations,config.sizefeat).foreach(println)}")
                LoggerHelper.logWarn(s"Result : ${baseResult.foreach(println)}")
            } 
            case None => System.exit(1)
        }
    }
}
