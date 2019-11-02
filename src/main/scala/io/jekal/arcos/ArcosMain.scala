package io.jekal.arcos

import io.jekal.arcos.udfs.Udfs
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ArcosMain {
  def main(args: Array[String]): Unit = {
    val stepName = if (args.size == 0) "stage1" else args(0)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    Udfs.register(spark)

    val pop = spark.read.
      option("header", true).
      option("nullValue", "X").
      option("mode", "FAILFAST").
      schema(Schemas.pop).
      csv("s3://arcos-opioid/opioids/census/sub-est2012.csv")
    val countyPop = pop.filter($"SUMLEV" === "050").
      withColumn("COUNTYNAME", regexp_replace(upper($"NAME"), "\\s+(CITY AND BOROUGH|COUNTY|BOROUGH|CENSUS AREA|MUNICIPALITY|PARISH)$", "")).
      withColumn("STATECODE", expr("getStateCode(STNAME)"))

    pop.createTempView("pop")
    countyPop.createTempView("countyPop")

    stepName match {
      case "stage1" => {
        val arcos = spark.read.
          option("header", true).
          option("sep", "\t").
          option("nullValue", "null").
          option("dateFormat", "MMddyyyy").
          option("mode", "FAILFAST").
          schema(Schemas.arcos).
          csv("s3://arcos-opioid/opioids/arcos_all_washpost.tsv.bz2").
          withColumn("spark_id", monotonically_increasing_id()).
          na.fill("", Seq("BUYER_STATE", "BUYER_COUNTY"))

        arcos.createTempView("arcos")

        val stage1 = spark.sql(
          """
            |select
            |    arcos.*,
            |    cp.*,
            |    case when cp.COUNTY is not null then TRUE else FALSE end as HAS_POPULATION
            |from arcos
            |    left join countyPop as cp
            |        on arcos.BUYER_STATE = cp.STATECODE
            |        and regexp_replace(arcos.BUYER_COUNTY, '\W', '') = regexp_replace(cp.COUNTYNAME, '\W', '')
            |""".stripMargin).cache()
        stage1.where($"HAS_POPULATION" === true).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/populated")
        stage1.where($"HAS_POPULATION" === false).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/missing")
        stage1.unpersist()
      }
      case "stage2" => {
        val stage_1_pop_missing = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/missing").where(col("BUYER_STATE") =!= "" && col("BUYER_COUNTY") =!= "")
        val arcos_fuzzy_populated = matchCounties(spark, countyPop, stage_1_pop_missing)
        arcos_fuzzy_populated.createTempView("arcos_fuzzy_populated")

        val stage2 = spark.sql(
          """
            |select
            |    *
            |from arcos_fuzzy_populated
            |union
            |select
            |    *
            |from arcos_s1_missing
            |where
            |    not exists (select 1 from arcos_fuzzy_populated where arcos_fuzzy_populated.spark_id = arcos_s1_missing.spark_id)
            |""".stripMargin).cache()
        stage2.where($"HAS_POPULATION" === true).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_2/populated")
        stage2.where($"HAS_POPULATION" === false).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_2/missing")
        stage2.unpersist()
      }
      case "stage3" => {
        val stage_2_pop_missing = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_2/missing")
        val zip = spark.read.
          option("header", true).
          option("mode", "FAILFAST").
          schema(Schemas.zip).
          csv("s3://arcos-opioid/opioids/zip_code_database.csv").
          withColumn("decommissioned", expr("case when decommissioned = 1 then TRUE else FALSE end")).
          withColumn("countyname", regexp_replace(upper($"county"), "\\s+(CITY AND BOROUGH|COUNTY|BOROUGH|CENSUS AREA|MUNICIPALITY|PARISH)$", ""))

        stage_2_pop_missing.createTempView("stage_2_pop_missing")
        zip.createTempView("zip")

        val stage3 = spark.sql(
          """
            |select
            |    arcos.BUYER_DEA_NO,
            |    arcos.BUYER_BUS_ACT,
            |    arcos.BUYER_NAME,
            |    arcos.BUYER_ADDL_CO_INFO,
            |    arcos.BUYER_ADDRESS1,
            |    arcos.BUYER_ADDRESS2,
            |    arcos.BUYER_CITY,
            |    arcos.BUYER_STATE,
            |    arcos.BUYER_ZIP,
            |    arcos.BUYER_COUNTY,
            |    arcos.TRANSACTION_CODE,
            |    arcos.DRUG_CODE,
            |    arcos.NDC_NO,
            |    arcos.DRUG_NAME,
            |    arcos.QUANTITY,
            |    arcos.UNIT,
            |    arcos.ACTION_INDICATOR,
            |    arcos.ORDER_FORM_NO,
            |    arcos.CORRECTION_NO,
            |    arcos.STRENGTH,
            |    arcos.TRANSACTION_DATE,
            |    arcos.CALC_BASE_WT_IN_GM,
            |    arcos.DOSAGE_UNIT,
            |    arcos.TRANSACTION_ID,
            |    arcos.Product_Name,
            |    arcos.Ingredient_Name,
            |    arcos.Measure,
            |    arcos.MME_Conversion_Factor,
            |    arcos.Combined_Labeler_Name,
            |    arcos.Revised_Company_Name,
            |    arcos.Reporter_family,
            |    arcos.dos_str,
            |    arcos.spark_id,
            |    cp.SUMLEV,
            |    cp.STATE,
            |    cp.COUNTY,
            |    cp.PLACE,
            |    cp.COUSUB,
            |    cp.CONCIT,
            |    cp.NAME,
            |    cp.STNAME,
            |    cp.CENSUS2010POP,
            |    cp.ESTIMATESBASE2010,
            |    cp.POPESTIMATE2010,
            |    cp.POPESTIMATE2011,
            |    cp.POPESTIMATE2012,
            |    cp.COUNTYNAME,
            |    cp.STATECODE,
            |    case when cp.COUNTY is not null then TRUE else FALSE end as HAS_POPULATION
            |from stage_2_pop_missing as arcos
            |    left join zip
            |        on arcos.BUYER_ZIP = zip.zip
            |    left join countyPop as cp
            |        on zip.state = cp.STATECODE
            |        and regexp_replace(zip.countyname, '\W', '') = regexp_replace(cp.COUNTYNAME, '\W', '')
            |""".stripMargin).cache()
        stage3.where($"HAS_POPULATION" === true).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_3/populated")
        stage3.where($"HAS_POPULATION" === false).write.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_3/missing")
        stage3.unpersist()
      }
      case "agg" => {
        val arcos_with_pop = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_2/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_3/populated").
          withColumn("POPESTIMATE2006", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2007", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2008", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2009", $"POPESTIMATE2010")

        arcos_with_pop.createTempView("arcos_with_pop")
        spark.sql(
          """
            |select
            |    *,
            |    pill_count / population as pill_count_per_capita
            |from (
            |    select
            |        STATE as state_fips,
            |        STATECODE as state_code,
            |        COUNTY as county_fips,
            |        NAME as county_name,
            |        concat(STATE, COUNTY) as state_county_fips,
            |        DRUG_NAME as drug_name,
            |        weekIndex(TRANSACTION_DATE) as week_index,
            |        yearindex(TRANSACTION_DATE) as year_index,
            |        getPopulation(TRANSACTION_DATE, POPESTIMATE2006, POPESTIMATE2007, POPESTIMATE2008, POPESTIMATE2009, POPESTIMATE2010, POPESTIMATE2011, POPESTIMATE2012) as population, -- using 2010 population for 2006-2009 as we don't have those datapoints
            |        dos_str as dosage_strength,
            |        cast(count(1) as double) as order_count,
            |        sum(QUANTITY) as pill_count,
            |        sum(QUANTITY * coalesce(dos_str, 1.0)) as normalized_pill_count
            |    from arcos_with_pop
            |    group by
            |        STATE,
            |        STATECODE,
            |        COUNTY,
            |        NAME,
            |        DRUG_NAME,
            |        weekIndex(TRANSACTION_DATE),
            |        yearindex(TRANSACTION_DATE),
            |        getPopulation(TRANSACTION_DATE, POPESTIMATE2006, POPESTIMATE2007, POPESTIMATE2008, POPESTIMATE2009, POPESTIMATE2010, POPESTIMATE2011, POPESTIMATE2012),
            |        dos_str
            |) x
            |""".stripMargin).write.parquet("s3://arcos-opioid/opioids/arcos_aggregated")
      }
      case "window" => {
        val arcos_agg = spark.read.parquet("s3://arcos-opioid/opioids/arcos_aggregated").na.drop
        arcos_agg.createTempView("arcos_agg")
        spark.sql(
          """
            |select
            |    *,
            |    toVector(collect_list(pill_count_per_capita) over w) as pill_count_per_capita_history
            |from arcos_agg
            |window w as (partition by state_county_fips, drug_name order by week_index asc rows between 13 preceding and 1 preceding)
            |""".stripMargin).write.parquet("s3://arcos-opioid/opioids/arcos_windowed")
      }
      case "dashboard" => {
        val arcos_with_pop = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_2/populated", "s3://arcos-opioid/opioids/arcos_with_pop/stage_3/populated").
          withColumn("POPESTIMATE2006", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2007", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2008", $"POPESTIMATE2010").
          withColumn("POPESTIMATE2009", $"POPESTIMATE2010")

        arcos_with_pop.createTempView("arcos_with_pop")

        val zip = spark.read.
          option("header", true).
          option("mode", "FAILFAST").
          schema(Schemas.zip).
          csv("s3://arcos-opioid/opioids/zip_code_database.csv").
          withColumn("decommissioned", expr("case when decommissioned = 1 then TRUE else FALSE end"))

        zip.createTempView("zip")

        spark.sql(
          """
            |select
            |    arcos.spark_id,
            |    arcos.BUYER_DEA_NO,
            |    arcos.BUYER_BUS_ACT,
            |    arcos.BUYER_NAME,
            |    arcos.BUYER_ADDL_CO_INFO,
            |    arcos.BUYER_ADDRESS1,
            |    arcos.BUYER_ADDRESS2,
            |    arcos.BUYER_CITY,
            |    arcos.BUYER_STATE,
            |    arcos.BUYER_ZIP,
            |    arcos.BUYER_COUNTY,
            |    arcos.TRANSACTION_CODE,
            |    arcos.DRUG_CODE,
            |    arcos.NDC_NO,
            |    arcos.DRUG_NAME,
            |    arcos.QUANTITY,
            |    arcos.UNIT,
            |    arcos.ACTION_INDICATOR,
            |    arcos.ORDER_FORM_NO,
            |    arcos.CORRECTION_NO,
            |    arcos.STRENGTH,
            |    arcos.TRANSACTION_DATE,
            |    arcos.CALC_BASE_WT_IN_GM,
            |    arcos.DOSAGE_UNIT,
            |    arcos.TRANSACTION_ID,
            |    arcos.Product_Name,
            |    arcos.Ingredient_Name,
            |    arcos.Measure,
            |    arcos.MME_Conversion_Factor,
            |    arcos.Combined_Labeler_Name,
            |    arcos.Revised_Company_Name,
            |    arcos.Reporter_family,
            |    arcos.dos_str,
            |    arcos.STATE as STATE_FIPS,
            |    arcos.COUNTY as COUNTY_FIPS,
            |    concat(arcos.STATE, arcos.COUNTY) as STATE_COUNTY_FIPS,
            |    arcos.NAME as COUNTY_NAME,
            |    getPopulation(arcos.TRANSACTION_DATE, arcos.POPESTIMATE2006, arcos.POPESTIMATE2007, arcos.POPESTIMATE2008, arcos.POPESTIMATE2009, arcos.POPESTIMATE2010, arcos.POPESTIMATE2011, arcos.POPESTIMATE2012) as population, -- using 2010 population for 2006-2009 as we don't have those datapoints
            |    zip.longitude as LONGITUDE,
            |    zip.latitude as LATITUDE
            |from arcos_with_pop arcos
            |    join zip
            |        on arcos.BUYER_ZIP = zip.zip
            |""".stripMargin).write.option("compression", "gzip").json("s3://arcos-opioid/opioids/arcos_dashboard")
      }
    }

    spark.stop()
  }

  def matchCounties(spark: SparkSession, countyPop: DataFrame, arcos: DataFrame) = {
    val arcosWithCounty = arcos.
      withColumn("COUNTY_VALUE", regexp_replace(col("BUYER_COUNTY"), "\\W", ""));
    arcos.createTempView("arcos_s1_missing")

    val county = countyPop.withColumn("COUNTY_VALUE", regexp_replace(col("COUNTYNAME"), "\\W", ""));
    val tokenizer = new RegexTokenizer().setPattern("").setMinTokenLength(0).setInputCol("COUNTY_VALUE").setOutputCol("COUNTY_VALUE_TOKENS")
    val ngram = new NGram().setN(3).setInputCol("COUNTY_VALUE_TOKENS").setOutputCol("COUNTY_VALUE_NGRAMS")
    val tf = new HashingTF().setInputCol("COUNTY_VALUE_NGRAMS").setOutputCol("COUNTY_VALUE_VECTORS")
    val lsh = new MinHashLSH().setNumHashTables(3).setInputCol("COUNTY_VALUE_VECTORS").setOutputCol("COUNTY_VALUE_LSH")
    val pipeline = new Pipeline().setStages(Array(tokenizer, ngram, tf, lsh))
    val model = pipeline.fit(county)

    val arcosHashed = model.transform(arcosWithCounty)
    val countyHashed = model.transform(county)

    val arcos_fuzzy = model.stages.last.asInstanceOf[MinHashLSHModel].approxSimilarityJoin(countyHashed, arcosHashed, 0.25D).where(col("datasetA.STATECODE") === col("datasetB.BUYER_STATE")).toDF
    arcos_fuzzy.createTempView("arcos_fuzzy")

    spark.sql(
      """
        |select
        |    arcos.REPORTER_DEA_NO,
        |    arcos.REPORTER_BUS_ACT,
        |    arcos.REPORTER_NAME,
        |    arcos.REPORTER_ADDL_CO_INFO,
        |    arcos.REPORTER_ADDRESS1,
        |    arcos.REPORTER_ADDRESS2,
        |    arcos.REPORTER_CITY,
        |    arcos.REPORTER_STATE,
        |    arcos.REPORTER_ZIP,
        |    arcos.REPORTER_COUNTY,
        |    arcos.BUYER_DEA_NO,
        |    arcos.BUYER_BUS_ACT,
        |    arcos.BUYER_NAME,
        |    arcos.BUYER_ADDL_CO_INFO,
        |    arcos.BUYER_ADDRESS1,
        |    arcos.BUYER_ADDRESS2,
        |    arcos.BUYER_CITY,
        |    arcos.BUYER_STATE,
        |    arcos.BUYER_ZIP,
        |    arcos.BUYER_COUNTY,
        |    arcos.TRANSACTION_CODE,
        |    arcos.DRUG_CODE,
        |    arcos.NDC_NO,
        |    arcos.DRUG_NAME,
        |    arcos.QUANTITY,
        |    arcos.UNIT,
        |    arcos.ACTION_INDICATOR,
        |    arcos.ORDER_FORM_NO,
        |    arcos.CORRECTION_NO,
        |    arcos.STRENGTH,
        |    arcos.TRANSACTION_DATE,
        |    arcos.CALC_BASE_WT_IN_GM,
        |    arcos.DOSAGE_UNIT,
        |    arcos.TRANSACTION_ID,
        |    arcos.Product_Name,
        |    arcos.Ingredient_Name,
        |    arcos.Measure,
        |    arcos.MME_Conversion_Factor,
        |    arcos.Combined_Labeler_Name,
        |    arcos.Revised_Company_Name,
        |    arcos.Reporter_family,
        |    arcos.dos_str,
        |    arcos.spark_id,
        |    cp.SUMLEV,
        |    cp.STATE,
        |    cp.COUNTY,
        |    cp.PLACE,
        |    cp.COUSUB,
        |    cp.CONCIT,
        |    cp.NAME,
        |    cp.STNAME,
        |    cp.CENSUS2010POP,
        |    cp.ESTIMATESBASE2010,
        |    cp.POPESTIMATE2010,
        |    cp.POPESTIMATE2011,
        |    cp.POPESTIMATE2012,
        |    cp.COUNTYNAME,
        |    cp.STATECODE,
        |    true as HAS_POPULATION
        |from (
        |    select
        |        datasetA as cp,
        |        datasetB as arcos,
        |        row_number() over (partition by datasetB.spark_id order by distCol asc) as rn
        |    from arcos_fuzzy
        |) x
        |where
        |    x.rn = 1
        |""".stripMargin)
  }
}

object StateMap extends Serializable {
  val map = Map(
    "ALABAMA" -> "AL",
    "ALASKA" -> "AK",
    "ARIZONA" -> "AZ",
    "ARKANSAS" -> "AR",
    "CALIFORNIA" -> "CA",
    "COLORADO" -> "CO",
    "CONNECTICUT" -> "CT",
    "DELAWARE" -> "DE",
    "DISTRICT OF COLUMBIA" -> "DC",
    "FLORIDA" -> "FL",
    "GEORGIA" -> "GA",
    "HAWAII" -> "HI",
    "IDAHO" -> "ID",
    "ILLINOIS" -> "IL",
    "INDIANA" -> "IN",
    "IOWA" -> "IA",
    "KANSAS" -> "KS",
    "KENTUCKY" -> "KY",
    "LOUISIANA" -> "LA",
    "MAINE" -> "ME",
    "MARYLAND" -> "MD",
    "MASSACHUSETTS" -> "MA",
    "MICHIGAN" -> "MI",
    "MINNESOTA" -> "MN",
    "MISSISSIPPI" -> "MS",
    "MISSOURI" -> "MO",
    "MONTANA" -> "MT",
    "NEBRASKA" -> "NE",
    "NEVADA" -> "NV",
    "NEW HAMPSHIRE" -> "NH",
    "NEW JERSEY" -> "NJ",
    "NEW MEXICO" -> "NM",
    "NEW YORK" -> "NY",
    "NORTH CAROLINA" -> "NC",
    "NORTH DAKOTA" -> "ND",
    "OHIO" -> "OH",
    "OKLAHOMA" -> "OK",
    "OREGON" -> "OR",
    "PENNSYLVANIA" -> "PA",
    "RHODE ISLAND" -> "RI",
    "SOUTH CAROLINA" -> "SC",
    "SOUTH DAKOTA" -> "SD",
    "TENNESSEE" -> "TN",
    "TEXAS" -> "TX",
    "UTAH" -> "UT",
    "VERMONT" -> "VT",
    "VIRGINIA" -> "VA",
    "WASHINGTON" -> "WA",
    "WEST VIRGINIA" -> "WV",
    "WISCONSIN" -> "WI",
    "WYOMING" -> "WY",

    // special cases
    "VIRGIN ISLANDS" -> "VI",
    "GUAM" -> "GU",
    "ARMED FORCES IN EUROPE" -> "AE",
    "PALAU" -> "PW",
    "PUERTO RICO" -> "PR",
    "NORTHERN MARIANAS" -> "MP"
  )

  def stateCode(state: String) = {
    val key = state.toUpperCase
    var stateCode = map(key)
    if (stateCode == null) {
      stateCode = if (map.values.exists(_ == key)) key else null
    }

    stateCode
  }
}

object Schemas extends Serializable {
  val arcos = StructType(Seq(
    StructField("REPORTER_DEA_NO", StringType),
    StructField("REPORTER_BUS_ACT", StringType),
    StructField("REPORTER_NAME", StringType),
    StructField("REPORTER_ADDL_CO_INFO", StringType),
    StructField("REPORTER_ADDRESS1", StringType),
    StructField("REPORTER_ADDRESS2", StringType),
    StructField("REPORTER_CITY", StringType),
    StructField("REPORTER_STATE", StringType),
    StructField("REPORTER_ZIP", StringType),
    StructField("REPORTER_COUNTY", StringType),
    StructField("BUYER_DEA_NO", StringType),
    StructField("BUYER_BUS_ACT", StringType),
    StructField("BUYER_NAME", StringType),
    StructField("BUYER_ADDL_CO_INFO", StringType),
    StructField("BUYER_ADDRESS1", StringType),
    StructField("BUYER_ADDRESS2", StringType),
    StructField("BUYER_CITY", StringType),
    StructField("BUYER_STATE", StringType),
    StructField("BUYER_ZIP", StringType),
    StructField("BUYER_COUNTY", StringType),
    StructField("TRANSACTION_CODE", StringType),
    StructField("DRUG_CODE", StringType),
    StructField("NDC_NO", StringType),
    StructField("DRUG_NAME", StringType),
    StructField("QUANTITY", DoubleType),
    StructField("UNIT", StringType),
    StructField("ACTION_INDICATOR", StringType),
    StructField("ORDER_FORM_NO", StringType),
    StructField("CORRECTION_NO", StringType),
    StructField("STRENGTH", StringType),
    StructField("TRANSACTION_DATE", DateType),
    StructField("CALC_BASE_WT_IN_GM", DoubleType),
    StructField("DOSAGE_UNIT", DoubleType),
    StructField("TRANSACTION_ID", StringType),
    StructField("Product_Name", StringType),
    StructField("Ingredient_Name", StringType),
    StructField("Measure", StringType),
    StructField("MME_Conversion_Factor", DoubleType),
    StructField("Combined_Labeler_Name", StringType),
    StructField("Revised_Company_Name", StringType),
    StructField("Reporter_family", StringType),
    StructField("dos_str", DoubleType)
  ))

  val pop = StructType(Seq(
    StructField("SUMLEV", StringType),
    StructField("STATE", StringType),
    StructField("COUNTY", StringType),
    StructField("PLACE", StringType),
    StructField("COUSUB", StringType),
    StructField("CONCIT", StringType),
    StructField("NAME", StringType),
    StructField("STNAME", StringType),
    StructField("CENSUS2010POP", LongType),
    StructField("ESTIMATESBASE2010", LongType),
    StructField("POPESTIMATE2010", LongType),
    StructField("POPESTIMATE2011", LongType),
    StructField("POPESTIMATE2012", LongType)
  ))

  val zip = StructType(Seq(
    StructField("zip", StringType),
    StructField("type", StringType),
    StructField("decommissioned", IntegerType),
    StructField("primary_city", StringType),
    StructField("acceptable_cities", StringType),
    StructField("unacceptable_cities", StringType),
    StructField("state", StringType),
    StructField("county", StringType),
    StructField("timezone", StringType),
    StructField("area_codes", StringType),
    StructField("world_region", StringType),
    StructField("country", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("irs_estimated_population_2015", LongType)
  ))
}
