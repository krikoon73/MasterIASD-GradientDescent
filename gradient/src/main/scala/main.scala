import org.apache.spark.{SparkConf, SparkContext}
import org.krikoon73.lib.{LoggerHelper, batchGD, SGD_RDD, batchGD_RDD, SGD_RDD_momentum}
import scopt.OptionParser

object main {
  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[CommandLineArgs]("GradientDescent") {
      head("Gradient Descent example", "1.0")
      opt[String]('a', "appname")
        .required().valueName("<SPARK Application Name>")
        .action((x, a) => a.copy(AppName = x))
        .text("Setting SPARK Application Name is required")
      opt[String]('m', "master")
        .required().valueName("<Cluster or Local>")
        .action((x, m) => m.copy(Master = x))
        .text("Setting local or cluster execution is required")
      opt[Double]('s', "stepsize")
        .required().valueName("<Gradient Descent step>")
        .action((x, s) => s.copy(stepsize = x))
        .text("Setting gradient descent step size is required")
      opt[Int]('i', "iterations")
        .required().valueName("<Gradient Descent epochs>")
        .action((x, i) => i.copy(iterations = x))
        .text("Setting gradient descent epochs is required")
      opt[Int]('f', "features")
        .required().valueName("<Gradient Descent features>")
        .action((x, i) => i.copy(sizefeat = x))
        .text("Setting gradient descent features is required")
    }

    def time[R](block: => (String, R)): R = {
      val t0 = System.currentTimeMillis()
      val result = block._2
      val t1 = System.currentTimeMillis()
      //println(block._1 + " took Elapsed time of " + (t1 - t0) + " Millis")
      LoggerHelper.logWarn(s"${block._1} took Elapsed time of ${t1 - t0} ms")
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
        //val dtrain = sc.parallelize(X.map(x=>(5*x+2,Array(x,1.0))))
        //val dtrain_raw = X.map(x => (x + 2, Array(x, 1.0)))
        val dtrain_raw = X.map(x => (5*x+2, Array(x, 1.0)))
        val m = dtrain_raw.length
        val dtrain = sc.parallelize(dtrain_raw)


        val loading = time("Loading dtrain", dtrain.count())
        LoggerHelper.logWarn(s"Number of elements for dtrain : ${loading}")
        LoggerHelper.logWarn(s"Stepsize : ${config.stepsize}")
        LoggerHelper.logWarn(s"Number of epochs : ${config.iterations}")
        //
        //val baseResult1 = time("Simple RDD Gradient Descent", batchGD_RDD(dtrain, config.stepsize, config.iterations, config.sizefeat, m))
        //LoggerHelper.logWarn(s"Result : w0 = ${baseResult1(0)} w1 = ${baseResult1(1)}")
        //
        val baseResult2 = time("RDD ** Stochastic Gradient Descent", SGD_RDD(dtrain, config.stepsize, config.iterations, config.sizefeat))
        LoggerHelper.logWarn(s"Result : w0 = ${baseResult2(0)} w1 = ${baseResult2(1)}")
        //
        val baseResult3 = time("RDD ** Momemtum Stochastique Gradient Descent", SGD_RDD_momentum(dtrain, config.sizefeat , config.stepsize, 0.9))
        LoggerHelper.logWarn(s"Result : w0 = ${baseResult3(0)} w1 = ${baseResult3(1)}")
        //
        //val baseResult11 = time("Simple Gradient Descent", batchGD(dtrain_raw, config.sizefeat , config.stepsize , config.iterations ))
        //LoggerHelper.logWarn(s"Result : w0 = ${baseResult11(0)} w1 = ${baseResult11(1)}")
      }
      case None => System.exit(1)
    }
  }
}
