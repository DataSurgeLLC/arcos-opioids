package io.jekal.arcos

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoderEstimator, StringIndexer, VectorAssembler, VectorIndexer}
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
        val (stateCodeIndexer, stateCodeConverter) = stringIndexer("state_code")
        val (stateCountyIndexer, stateCountyConverter) = stringIndexer("state_county_fips")

        val oneHot = new OneHotEncoderEstimator().
          setInputCols(Array("drug_name_indexed", "state_code_indexed", "state_county_fips_indexed")).
          setOutputCols(Array("drug_name_one_hot", "state_code_one_hot", "state_county_fips_one_hot")).
          setHandleInvalid("keep")

        val assembler = new VectorAssembler().
          setInputCols(Array("drug_name_one_hot", "state_code_one_hot", "state_county_fips_one_hot", "week_index", "year_index", "population", "dosage_strength", "pill_count_per_capita_history")).
          setOutputCol("assembledFeatures")

        val indexer = new VectorIndexer().
          setInputCol("assembledFeatures").
          setOutputCol("features").
          setHandleInvalid("keep").
          setMaxCategories(4)

        val pill_count_per_capita_pipeline = pipelineIt("pill_count_per_capita")

        val stages = Array(drugNameIndexer, stateCodeIndexer, stateCountyIndexer, oneHot, assembler, indexer, pill_count_per_capita_pipeline, drugNameConverter, stateCodeConverter, stateCountyConverter)
        val pipeline = new Pipeline().setStages(stages)

        val model = pipeline.fit(trainingData)
        model.save("s3://arcos-opioid/opioids/models")
      }
      case "predict" => {
        val model = PipelineModel.load("s3://arcos-opioid/opioids/models")
        val predictions = model.transform(testData)
        val trainingPredictions = model.transform(trainingData)

        predictions.write.parquet("s3://arcos-opioid/opioids/predictions/test")
        trainingPredictions.write.parquet("s3://arcos-opioid/opioids/predictions/training")
      }
      case "stats" => {
        val model = PipelineModel.load("s3://arcos-opioid/opioids/models")
        val crossValidatorIndex = 6
        println(model.stages(crossValidatorIndex).asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(0).explainParams())

        val predictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/test")
        val trainingPredictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/training")

        evaluate("pill_count_per_capita", predictions, trainingPredictions)

        arcos_windowed.describe().show(false)
        predictions.show(false)
        trainingPredictions.show(false)

        printFatureImportances("pill_count_per_capita", model.stages(crossValidatorIndex).asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[RandomForestRegressionModel], predictions)
      }
      case "report" => {
        val predictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/test")
        val trainingPredictions = spark.read.parquet("s3://arcos-opioid/opioids/predictions/training")
        val allPredictions = trainingPredictions.union(predictions)
        val stdDev = allPredictions.select(stddev($"pill_count_per_capita")).collect()(0).getAs[Double](0)
        println(s"Std Dev of pill_count_per_capita: $stdDev")
        val anomalies = allPredictions.where(!$"pill_count_per_capita".between($"pill_count_per_capita_prediction" - stdDev, $"pill_count_per_capita_prediction" + stdDev))
        anomalies.cache()
        anomalies.show(false)
        anomalies.write.parquet("s3://arcos-opioid/opioids/anomalies")
        anomalies.createTempView("anomalies")
        spark.sql(
          """
            |select
            |    STATE_COUNTY_FIPS,
            |    count(*) as anomalies_count,
            |    sum(abs(pill_count_per_capita - pill_count_per_capita_prediction)) as pill_count_per_capita_delta
            |from anomalies
            |group by
            |    STATE_COUNTY_FIPS
            |order by
            |    count(*) desc
            |limit 100
            |""".stripMargin).write.parquet("s3://arcos-opioid/opioids/ranked_anomalies")
        anomalies.unpersist()
      }
    }
  }

  def pipelineIt(labelCol: String) = {
    val regressor = randomForestRegressor(labelCol)
    val pipeline = new Pipeline().setStages(Array(regressor))

    val paramGrid = new ParamGridBuilder().
      addGrid(regressor.numTrees, Array(50, 100)).
      addGrid(regressor.maxDepth, Array(7, 11)).
      build()

    val ev = evaluator(labelCol)

    new CrossValidator().
      setEstimator(pipeline).
      setEvaluator(ev).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(5).
      setParallelism(4)
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
