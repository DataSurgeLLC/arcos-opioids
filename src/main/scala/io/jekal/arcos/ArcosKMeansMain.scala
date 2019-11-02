package io.jekal.arcos

import io.jekal.arcos.udfs.Udfs
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object ArcosKMeansMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    Udfs.register(spark)

    val arg = args(0)

    arg match {
      case "prep" => {
        val arcos_with_pop = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_2/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_3/populated").
          withColumn("POPESTIMATE2006", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2007", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2008", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2009", $"POPESTIMATE2010")

        arcos_with_pop.createTempView("arcos_with_pop")

        spark.sql(
          """
            |select
            |    REPORTER_BUS_ACT,
            |    REPORTER_ZIP,
            |    BUYER_BUS_ACT,
            |    BUYER_ZIP,
            |    DRUG_NAME,
            |    ACTION_INDICATOR,
            |    STRENGTH,
            |    weekIndex(TRANSACTION_DATE) as week_index,
            |    Combined_Labeler_Name,
            |    Revised_Company_Name,
            |    Reporter_family,
            |    dos_str,
            |    getPopulation(TRANSACTION_DATE, POPESTIMATE2006, POPESTIMATE2007, POPESTIMATE2008, POPESTIMATE2009, POPESTIMATE2010, POPESTIMATE2011, POPESTIMATE2012) as population,
            |    QUANTITY
            |from arcos_with_pop
            |""".stripMargin).
          na.fill("N/A", Array("REPORTER_BUS_ACT", "REPORTER_ZIP", "BUYER_BUS_ACT", "BUYER_ZIP", "DRUG_NAME", "ACTION_INDICATOR", "STRENGTH", "Combined_Labeler_Name", "Revised_Company_Name", "Reporter_family")).
          na.fill(0D, Array("dos_str")).
          na.fill(0L, Array("week_index", "population", "QUANTITY")).
          write.parquet("s3://arcos-opioid/opioids/arcos_kmeans")
      }
      case "train" => {
        val all = spark.read.parquet("s3://arcos-opioid/opioids/arcos_kmeans")
        val arcos = all.sample(0.25D, 42L)
        arcos.persist(StorageLevel.MEMORY_AND_DISK)
        (5500 to 10000 by 500).foreach(k => train(arcos, k))
      }
      case "explore" => {
        val all = spark.read.parquet("s3://arcos-opioid/opioids/arcos_kmeans")
        val arcos = all.sample(0.25D, 42L)
        arcos.persist(StorageLevel.MEMORY_AND_DISK)
        val rowCount = arcos.count()

        println("kSize\tcost\tweightedEntropy")
        val metrics = (3500 to 5000 by 500).
          map(k => (k, computeScore(spark, arcos, k, rowCount))).
          map(stats => {
            println(s"${stats._1}\t${stats._2._1}\t${stats._2._2}")
            (stats._1, stats._2._1, stats._2._2)
          }).
          toDF("kSize", "cost", "weightedEntropy")
        metrics.coalesce(1).write.option("header", true).mode("append").csv("s3://arcos-opioid/opioids/kmeans/metrics")
        arcos.unpersist()
      }
      case "train_full" => {

      }
      case "predict" => {

      }
      case "stats" => {

      }
      case "report" => {

      }
    }
  }

  def train(arcos: DataFrame, k: Int) = {
    val (reporter_bus_act_indexer, reporter_bus_act_converter) = stringIndexer("REPORTER_BUS_ACT")
    val (reporter_zip_indexer, reporter_zip_converter) = stringIndexer("REPORTER_ZIP")
    val (buyer_bus_act_indexer, buyer_bus_act_converter) = stringIndexer("BUYER_BUS_ACT")
    val (buyer_zip_indexer, buyer_zip_converter) = stringIndexer("BUYER_ZIP")
    val (drug_name_indexer, drug_name_converter) = stringIndexer("DRUG_NAME")
    val (action_indicator_indexer, action_indicator_converter) = stringIndexer("ACTION_INDICATOR")
    val (combined_labeler_name_indexer, combined_labeler_name_converter) = stringIndexer("Combined_Labeler_Name")
    val (revised_company_name_indexer, revised_company_name_converter) = stringIndexer("Revised_Company_Name")
    val (reporter_family_indexer, reporter_family_converter) = stringIndexer("Reporter_family")
    val (strength_indexer, strength_converter) = stringIndexer("STRENGTH")

    val oneHot = new OneHotEncoderEstimator().
      setInputCols(Array("REPORTER_BUS_ACT_indexed", "REPORTER_ZIP_indexed", "BUYER_BUS_ACT_indexed", "BUYER_ZIP_indexed", "DRUG_NAME_indexed", "ACTION_INDICATOR_indexed", "Combined_Labeler_Name_indexed", "Revised_Company_Name_indexed", "Reporter_family_indexed", "STRENGTH_indexed")).
      setOutputCols(Array("REPORTER_BUS_ACT_one_hot", "REPORTER_ZIP_one_hot", "BUYER_BUS_ACT_one_hot", "BUYER_ZIP_one_hot", "DRUG_NAME_one_hot", "ACTION_INDICATOR_one_hot", "Combined_Labeler_Name_one_hot", "Revised_Company_Name_one_hot", "Reporter_family_one_hot", "STRENGTH_one_hot")).
      setHandleInvalid("keep")

    val assembler = new VectorAssembler().
      setInputCols(Array("REPORTER_BUS_ACT_one_hot", "REPORTER_ZIP_one_hot", "BUYER_BUS_ACT_one_hot", "BUYER_ZIP_one_hot", "DRUG_NAME_one_hot", "ACTION_INDICATOR_one_hot", "Combined_Labeler_Name_one_hot", "Revised_Company_Name_one_hot", "Reporter_family_one_hot", "STRENGTH_one_hot", "week_index", "dos_str", "population", "QUANTITY")).
      setOutputCol("assembledFeatures")

    val scaler = new StandardScaler().
      setInputCol("assembledFeatures").
      setOutputCol("scaledFeatures").
      setWithStd(true).
      setWithMean(false)

    val kmeans = new KMeans().
      setSeed(42L).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatures").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(Array(reporter_bus_act_indexer, reporter_zip_indexer, buyer_bus_act_indexer, buyer_zip_indexer, drug_name_indexer, action_indicator_indexer, combined_labeler_name_indexer, revised_company_name_indexer, reporter_family_indexer, strength_indexer,
      oneHot, assembler, scaler, kmeans,
      reporter_bus_act_converter, reporter_zip_converter, buyer_bus_act_converter, buyer_zip_converter, drug_name_converter, action_indicator_converter, combined_labeler_name_converter, revised_company_name_converter, reporter_family_converter, strength_converter))
    val pipelineModel = pipeline.fit(arcos)
    pipelineModel.save(s"s3://arcos-opioid/opioids/kmeans/model/$k")
  }

  def computeScore(spark: SparkSession, arcos: DataFrame, k: Int, rowCount: Long) = {
    import spark.implicits._

    val model = PipelineModel.load(s"s3://arcos-opioid/opioids/kmeans/model/$k")

    val transformed = model.transform(arcos)
    transformed.persist(StorageLevel.MEMORY_AND_DISK)

    val score = model.stages(13).asInstanceOf[KMeansModel].computeCost(transformed) / rowCount

    val clusterLabel = transformed.select("cluster", "QUANTITY").as[(Int, String)]
    val weightedClusterEntropy = clusterLabel.groupByKey { case (cluster, _) => cluster }.
      mapGroups { case (_, clusterLabels) =>
        val labels = clusterLabels.map { case (_, label) => label }.toSeq
        val labelCounts = labels.groupBy(identity).values.map(_.size)
        labels.size * entropy(labelCounts)
      }.collect()

    val entropyScore = weightedClusterEntropy.sum / rowCount

    transformed.unpersist()

    (score, entropyScore)
  }

  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n = values.map(_.toDouble).sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }

  def stringIndexer(inputCol: String) = Utils.stringIndexer(inputCol)
}
