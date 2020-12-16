// Import
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.rdd._

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder} 
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel

object diamonds {
  def main(args : Array[String]):Unit = {
    println("START PROCESSING ...")
    val spark = SparkSession.builder.appName("Application DIAMONDS").getOrCreate()
    val datapath = "data/diamonds.csv"
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load(datapath)
      .withColumn("priceOutputVar", col("price").cast("double"))
      .cache()

    // dealing with categorical feautures
    val categoricalVariables = Array("cut", "color", "clarity") 
    val categoricalIndexers = categoricalVariables
      .map(i => new StringIndexer().setInputCol(i).setOutputCol(i+"Index")) 
    val categoricalEncoders = categoricalVariables
      .map(e => new OneHotEncoder().setInputCol(e + "Index").setOutputCol(e + "Vec"))
    
     //asembling all of our features
    val assembler = new VectorAssembler()
      .setInputCols(Array("cutVec", "colorVec", "clarityVec", "carat", "depth")) .setOutputCol("features")

    // Building the Model and Parameter Grid - as seen on the slides
    val lr = new LinearRegression().setLabelCol("priceOutputVar") .setFeaturesCol("features")
    val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 1.0))
      .build()
   
    // the pipeline
    val steps: Array[org.apache.spark.ml.PipelineStage] = categoricalIndexers ++ categoricalEncoders ++ Array(assembler, lr)
    val pipeline = new Pipeline().setStages(steps)

    // training and evaluation
    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol("priceOutputVar")) .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)

    // creating a holdout testset and training
    val Array(training, test) = data.randomSplit(Array(0.75, 0.25), seed = 12345)
    val model = tvs.fit(training)

    // applying on test set and displayin estimators
    val holdout = model.transform(test).select("prediction", "priceOutputVar")
    // need a type conversion for RegressionMetrics
    val rm = new RegressionMetrics(holdout.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))) 
    println("sqrt(MSE): " + Math.sqrt(rm.meanSquaredError))
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")

    //let's see coefficents of best model - will see/discuss this in the next class

    // first extract the best model 

    val bestModel = model.bestModel match {
      case pm: PipelineModel => Some(pm)
      case _ => None
    }

    val lrm = bestModel
      .map(_.stages.collect { case lrm: LinearRegressionModel => lrm })
      .flatMap(_.headOption)

    lrm.map(m => (m.intercept, m.coefficients))

    // display and plot prices and estimated prices
    /*lrm match {
      case Some(v) => display(v, model.transform(test).select("features","priceOutputVar"), plotType="fittedVsResiduals") 
      case None => None
    }
    */
  }
}

