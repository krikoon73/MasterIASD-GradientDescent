import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.{Level, Logger}
import org.krikoon73.lib.BaseGradient
import org.krikoon73.lib.LoggerHelper

object GradientDescent {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TestGradientDescent").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        //val LOG = Logger.getLogger(this.getClass)  
        

        //LOG.warn("STARTING PROGAM") 
        LoggerHelper.logWarn("STARTING PROGRAM")
        val X = (1.0 to 10000).by(2).toArray
        val dtrain = sc.parallelize(X.map(x=>(5*x+2,Array(x,1.0))))
        //LOG.warn(s"dtrain : ${dtrain.count()}")
        LoggerHelper.logWarn(s"dtrain : ${dtrain.count()}")
        //LOG.warn(s"Result : ${BaseGradient(dtrain,0.00000000001,10000,2).foreach(println)}")
        LoggerHelper.logWarn(s"Result : ${BaseGradient(dtrain,0.00000000001,10000,2).foreach(println)}")
    }
}
