package io.jekal.arcos

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.stddev

object ArcosModelMain {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val arg = args(0)

    val arcos_windowed = spark.read.parquet("s3://arcos-opioid/opioids/arcos_windowed").na.drop
    val Array(trainingData, testData) = arcos_windowed.randomSplit(Array(0.7, 0.3), 42L)

    arg match {
      case "train" => {
        val (drugNameIndexer, drugNameConverter) = stringIndexer("drug_name")

        val assembler = new VectorAssembler().
          setInputCols(Array("drug_name_indexed", "week_index", "year_index", "population", "dosage_strength", "order_count_history", "pill_count_history", "normalized_pill_count_history")).
          setOutputCol("assembledFeatures")

        val indexer = new VectorIndexer().
          setInputCol("assembledFeatures").
          setOutputCol("features").
          setMaxCategories(4)

        // val order_count_pipeline = pipelineIt("order_count")
        val pill_count_pipeline = pipelineIt("pill_count")
        // val normalized_pill_count_pipeline = pipelineIt("normalized_pill_count")

        val pipeline = new Pipeline().setStages(Array(drugNameIndexer, assembler, indexer, /* order_count_pipeline, */ pill_count_pipeline, /* normalized_pill_count_pipeline, */ drugNameConverter))

        val model = pipeline.fit(trainingData)
        model.save("s3://arcos-opioid/opioids/model")

        val predictions = model.transform(testData)
        val trainingPredictions = model.transform(trainingData)

        predictions.write.parquet("s3://arcos-opioid/opioids/predictions/test")
        trainingPredictions.write.parquet("s3://arcos-opioid/opioids/predictions/training")
      }
      case "stats" => {
        val model = PipelineModel.load("s3://arcos-opioid/opioids/model")
        println(model.stages(3).asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(0).explainParams())

        val predictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/test")
        val trainingPredictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/training")

        // evaluate("order_count", predictions, trainingPredictions)
        evaluate("pill_count", predictions, trainingPredictions)
        // evaluate("normalized_pill_count", predictions, trainingPredictions)

        arcos_windowed.describe().show(false)

        // printFatureImportances("order_count", model.stages(3).asInstanceOf[PipelineModel].stages(0).asInstanceOf[RandomForestRegressionModel], predictions)
        printFatureImportances("pill_count", model.stages(/* 4 */ 3).asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[RandomForestRegressionModel], predictions)
        // printFatureImportances("normalized_pill_count", model.stages(5).asInstanceOf[PipelineModel].stages(0).asInstanceOf[RandomForestRegressionModel], predictions)
      }
      case "report" => {
        val predictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/test")
        val trainingPredictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/training")
        val allPredictions = trainingPredictions.union(predictions)
        val anomalies = allPredictions.where(!$"pill_count".between($"pill_count_prediction" / 2, $"pill_count_prediction" * 2))
        anomalies.show(false)
        anomalies.write.parquet("s3://arcos-opioid/opioids/anomalies")
      }
    }
  }

  def pipelineIt(labelCol: String) = {
    val regressor = randomForestRegressor(labelCol)
    val pipeline = new Pipeline().setStages(Array(regressor))

    val paramGrid = new ParamGridBuilder().
      addGrid(regressor.numTrees, Array(20, 50, 100)).
      addGrid(regressor.maxDepth, Array(3, 7, 11)).
      addGrid(regressor.minInfoGain, Array(0.0001, 0.001)).
      build()

    val ev = evaluator(labelCol)

    new CrossValidator().
      setEstimator(pipeline).
      setEvaluator(ev).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(10).
      setParallelism(2)
  }

  def randomForestRegressor(labelCol: String) = {
    val rf = new RandomForestRegressor().
      setLabelCol(labelCol).
      setFeaturesCol("features").
      setPredictionCol(s"${labelCol}_prediction")
    rf
  }

  def stringIndexer(inputCol: String) = {
    val indexer = new StringIndexer().
      setInputCol(inputCol).
      setOutputCol(s"${inputCol}_indexed").
      setHandleInvalid("keep")

    val indexToString = new IndexToString().
      setInputCol(s"${inputCol}_indexed").
      setOutputCol(s"${inputCol}_converted")
    (indexer, indexToString)
  }

  def evaluate(labelCol: String, predictions: DataFrame, trainingPredictions: DataFrame) = {
    val ev = evaluator(labelCol)
    val testRmse = ev.evaluate(predictions)
    val trainingRmse = ev.evaluate(trainingPredictions)

    println(s"Root Mean Squared Error (RMSE) on test data ($labelCol) = $testRmse")
    println(s"Root Mean Squared Error (RMSE) on training data ($labelCol) = $trainingRmse")
  }

  def evaluator(labelCol: String) = new RegressionEvaluator().setLabelCol(labelCol).setPredictionCol(s"${labelCol}_prediction").setMetricName("rmse")

  def printFatureImportances(name: String, model: RandomForestRegressionModel, predictions: DataFrame) = {
    println(s"Model feature importances for: $name")
    val featureMetadata = predictions.schema("features").metadata
    val attrs = featureMetadata.getMetadata("ml_attr").getMetadata("attrs")
    val f: Metadata => (Long, String) = (m => (m.getLong("idx"), m.getString("name")))
    // val nominalFeatures = attrs.getMetadataArray("nominal").map(f)
    val numericFeatures = attrs.getMetadataArray("numeric").map(f)
    val binaryFeatures = attrs.getMetadataArray("binary").map(f)
    val features = (numericFeatures ++ /* nominalFeatures ++ */ binaryFeatures).sortBy(_._1)

    val importances = model.featureImportances.toArray.zip(features).map(x=>(x._2._2, x._1)).sortBy(-_._2)
    importances.foreach(println(_))
  }
}
