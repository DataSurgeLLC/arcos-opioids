stupid docker-spark parquet issue:
```
./fix_parquet.sh
```

```
docker-compose up -d --scale worker=5
docker-compose exec master /bin/bash
```

```
val df = spark.read.option("header", "true").option("sep", "\t").csv("/tmp/data/arcos_all_washpost.tsv")
df.sample(0.001).coalesce(1).write.option("header", "true").option("sep", "\t").csv("/tmp/data/arcos_sample.tsv")

```

```
df.columns.map(println(_))
columns:
REPORTER_DEA_NO
REPORTER_BUS_ACT
REPORTER_NAME
REPORTER_ADDL_CO_INFO
REPORTER_ADDRESS1
REPORTER_ADDRESS2
REPORTER_CITY
REPORTER_STATE
REPORTER_ZIP
REPORTER_COUNTY
BUYER_DEA_NO
BUYER_BUS_ACT
BUYER_NAME
BUYER_ADDL_CO_INFO
BUYER_ADDRESS1
BUYER_ADDRESS2
BUYER_CITY
BUYER_STATE
BUYER_ZIP
BUYER_COUNTY
TRANSACTION_CODE
DRUG_CODE
NDC_NO
DRUG_NAME
QUANTITY
UNIT
ACTION_INDICATOR
ORDER_FORM_NO
CORRECTION_NO
STRENGTH
TRANSACTION_DATE
CALC_BASE_WT_IN_GM
DOSAGE_UNIT
TRANSACTION_ID
Product_Name
Ingredient_Name
Measure
MME_Conversion_Factor
Combined_Labeler_Name
Revised_Company_Name
Reporter_family
dos_str
```

```
df.columns.map(c => df.select(approxCountDistinct(col(c))).show())
+--------------------------------------+                                        
|approx_count_distinct(REPORTER_DEA_NO)|
+--------------------------------------+
|                                   649|
+--------------------------------------+

+---------------------------------------+                                       
|approx_count_distinct(REPORTER_BUS_ACT)|
+---------------------------------------+
|                                      4|
+---------------------------------------+

+------------------------------------+                                          
|approx_count_distinct(REPORTER_NAME)|
+------------------------------------+
|                                 510|
+------------------------------------+

+--------------------------------------------+                                  
|approx_count_distinct(REPORTER_ADDL_CO_INFO)|
+--------------------------------------------+
|                                         116|
+--------------------------------------------+

+----------------------------------------+                                      
|approx_count_distinct(REPORTER_ADDRESS1)|
+----------------------------------------+
|                                     583|
+----------------------------------------+

+----------------------------------------+                                      
|approx_count_distinct(REPORTER_ADDRESS2)|
+----------------------------------------+
|                                     213|
+----------------------------------------+

+------------------------------------+                                          
|approx_count_distinct(REPORTER_CITY)|
+------------------------------------+
|                                 447|
+------------------------------------+

+-------------------------------------+                                         
|approx_count_distinct(REPORTER_STATE)|
+-------------------------------------+
|                                   53|
+-------------------------------------+

+-----------------------------------+                                           
|approx_count_distinct(REPORTER_ZIP)|
+-----------------------------------+
|                                527|
+-----------------------------------+

+--------------------------------------+                                        
|approx_count_distinct(REPORTER_COUNTY)|
+--------------------------------------+
|                                   247|
+--------------------------------------+

+-----------------------------------+                                           
|approx_count_distinct(BUYER_DEA_NO)|
+-----------------------------------+
|                             140436|
+-----------------------------------+

+------------------------------------+                                          
|approx_count_distinct(BUYER_BUS_ACT)|
+------------------------------------+
|                                   6|
+------------------------------------+

+---------------------------------+                                             
|approx_count_distinct(BUYER_NAME)|
+---------------------------------+
|                           106961|
+---------------------------------+

+-----------------------------------------+                                     
|approx_count_distinct(BUYER_ADDL_CO_INFO)|
+-----------------------------------------+
|                                    45354|
+-----------------------------------------+

+-------------------------------------+                                         
|approx_count_distinct(BUYER_ADDRESS1)|
+-------------------------------------+
|                               127447|
+-------------------------------------+

+-------------------------------------+                                         
|approx_count_distinct(BUYER_ADDRESS2)|
+-------------------------------------+
|                                31051|
+-------------------------------------+

+---------------------------------+                                             
|approx_count_distinct(BUYER_CITY)|
+---------------------------------+
|                            11269|
+---------------------------------+

+----------------------------------+                                            
|approx_count_distinct(BUYER_STATE)|
+----------------------------------+
|                                59|
+----------------------------------+

+--------------------------------+                                              
|approx_count_distinct(BUYER_ZIP)|
+--------------------------------+
|                           17569|
+--------------------------------+

+-----------------------------------+                                           
|approx_count_distinct(BUYER_COUNTY)|
+-----------------------------------+
|                               1748|
+-----------------------------------+

+---------------------------------------+                                       
|approx_count_distinct(TRANSACTION_CODE)|
+---------------------------------------+
|                                      1|
+---------------------------------------+

+--------------------------------+                                              
|approx_count_distinct(DRUG_CODE)|
+--------------------------------+
|                               2|
+--------------------------------+

+-----------------------------+                                                 
|approx_count_distinct(NDC_NO)|
+-----------------------------+
|                         4224|
+-----------------------------+

+--------------------------------+                                              
|approx_count_distinct(DRUG_NAME)|
+--------------------------------+
|                               2|
+--------------------------------+

+-------------------------------+                                               
|approx_count_distinct(QUANTITY)|
+-------------------------------+
|                           1223|
+-------------------------------+

+---------------------------+                                                   
|approx_count_distinct(UNIT)|
+---------------------------+
|                          4|
+---------------------------+

+---------------------------------------+                                       
|approx_count_distinct(ACTION_INDICATOR)|
+---------------------------------------+
|                                      4|
+---------------------------------------+

+------------------------------------+                                          
|approx_count_distinct(ORDER_FORM_NO)|
+------------------------------------+
|                            21352522|
+------------------------------------+

+------------------------------------+                                          
|approx_count_distinct(CORRECTION_NO)|
+------------------------------------+
|                               35456|
+------------------------------------+

+-------------------------------+                                               
|approx_count_distinct(STRENGTH)|
+-------------------------------+
|                            353|
+-------------------------------+

+---------------------------------------+                                       
|approx_count_distinct(TRANSACTION_DATE)|
+---------------------------------------+
|                                   2649|
+---------------------------------------+

+-----------------------------------------+                                     
|approx_count_distinct(CALC_BASE_WT_IN_GM)|
+-----------------------------------------+
|                                     5541|
+-----------------------------------------+

+----------------------------------+                                            
|approx_count_distinct(DOSAGE_UNIT)|
+----------------------------------+
|                              3389|
+----------------------------------+

+-------------------------------------+                                         
|approx_count_distinct(TRANSACTION_ID)|
+-------------------------------------+
|                             11737372|
+-------------------------------------+

+-----------------------------------+                                           
|approx_count_distinct(Product_Name)|
+-----------------------------------+
|                               1019|
+-----------------------------------+

+--------------------------------------+                                        
|approx_count_distinct(Ingredient_Name)|
+--------------------------------------+
|                                     3|
+--------------------------------------+

+------------------------------+                                                
|approx_count_distinct(Measure)|
+------------------------------+
|                             1|
+------------------------------+

+--------------------------------------------+                                  
|approx_count_distinct(MME_Conversion_Factor)|
+--------------------------------------------+
|                                           2|
+--------------------------------------------+

+--------------------------------------------+                                  
|approx_count_distinct(Combined_Labeler_Name)|
+--------------------------------------------+
|                                          94|
+--------------------------------------------+

+-------------------------------------------+                                   
|approx_count_distinct(Revised_Company_Name)|
+-------------------------------------------+
|                                         88|
+-------------------------------------------+

+--------------------------------------+                                        
|approx_count_distinct(Reporter_family)|
+--------------------------------------+
|                                   390|
+--------------------------------------+

+------------------------------+                                                
|approx_count_distinct(dos_str)|
+------------------------------+
|                            19|
+------------------------------+
```

```
Seq("REPORTER_BUS_ACT", "BUYER_BUS_ACT", "BUYER_STATE", "TRANSACTION_CODE", "DRUG_CODE", "DRUG_NAME", "ACTION_INDICATOR", "Ingredient_Name", "Measure", "MME_Conversion_Factor", "dos_str").map(c => df.select(col(c)).distinct().show(100, false))
+---------------------+                                                         
|REPORTER_BUS_ACT     |
+---------------------+
|DISTRIBUTOR          |
|CHEMICAL MANUFACTURER|
|REVERSE DISTRIB      |
|MANUFACTURER         |
+---------------------+

+-------------------+                                                           
|BUYER_BUS_ACT      |
+-------------------+
|CHAIN PHARMACY     |
|PRACTITIONER-DW/30 |
|PRACTITIONER-DW/100|
|RETAIL PHARMACY    |
|PRACTITIONER       |
|PRACTITIONER-DW/275|
+-------------------+

+-----------+                                                                   
|BUYER_STATE|
+-----------+
|AZ         |
|SC         |
|LA         |
|MN         |
|NJ         |
|DC         |
|OR         |
|VA         |
|RI         |
|KY         |
|WY         |
|NH         |
|MI         |
|NV         |
|WI         |
|ID         |
|CA         |
|CT         |
|NE         |
|MT         |
|NC         |
|VT         |
|MD         |
|DE         |
|MO         |
|VI         |
|IL         |
|ME         |
|GU         |*Guam
|ND         |
|WA         |
|MS         |
|AL         |
|IN         |
|AE         |*Armed Forces in Europe
|OH         |
|TN         |
|NM         |
|IA         |
|PA         |
|SD         |
|NY         |
|TX         |
|WV         |
|GA         |
|MA         |
|KS         |
|CO         |
|FL         |
|AK         |
|AR         |
|PW         |*Palau
|OK         |
|PR         |*Puerto Rico
|MP         |*Northern Marianas
|UT         |
|HI         |
+-----------+

+----------------+                                                              
|TRANSACTION_CODE|
+----------------+
|S               |
+----------------+

+---------+                                                                     
|DRUG_CODE|
+---------+
|9143     |
|9193     |
+---------+

+-----------+                                                                   
|DRUG_NAME  |
+-----------+
|HYDROCODONE|
|OXYCODONE  |
+-----------+

+----------------+                                                              
|ACTION_INDICATOR|
+----------------+
|D               |
|A               |
|I               |
|null            |
+----------------+

+---------------------------------------------------+                           
|Ingredient_Name                                    |
+---------------------------------------------------+
|HYDROCODONE BITARTRATE HEMIPENTAHYDRATE            |
|POLY ETHYLENE GLYCOL OXYCODOL (MPEG 6-ALPHA-OXYCODO|
|OXYCODONE HYDROCHLORIDE                            |
+---------------------------------------------------+

+-------+                                                                       
|Measure|
+-------+
|TAB    |
+-------+

+---------------------+                                                         
|MME_Conversion_Factor|
+---------------------+
|1.0                  |
|1.5                  |
+---------------------+

+-------+                                                                       
|dos_str|
+-------+
|50.0   |
|20.0   |
|15.0   |
|45.0   |
|200.0  |
|120.0  |
|7.5    |
|2.5    |
|10.0   |
|400.0  |
|5.0    |
|100.0  |
|4.8355 |
|5.35   |
|60.0   |
|30.0   |
|80.0   |
|null   |
|40.0   |
+-------+

group by state, county, year, month_index, week_index, pill_count

val pop = spark.read.option("header", true).csv("/tmp/data/census/sub-est2012.csv")
val countyPop = pop.filter($"SUMLEV" === "050")

pop.select(upper($"STNAME")).distinct.orderBy($"STNAME").show(100, false)
+--------------------+                                                          
|upper(STNAME)       |
+--------------------+
|ALABAMA             |
|ALASKA              |
|ARIZONA             |
|ARKANSAS            |
|CALIFORNIA          |
|COLORADO            |
|CONNECTICUT         |
|DELAWARE            |
|DISTRICT OF COLUMBIA|
|FLORIDA             |
|GEORGIA             |
|HAWAII              |
|IDAHO               |
|ILLINOIS            |
|INDIANA             |
|IOWA                |
|KANSAS              |
|KENTUCKY            |
|LOUISIANA           |
|MAINE               |
|MARYLAND            |
|MASSACHUSETTS       |
|MICHIGAN            |
|MINNESOTA           |
|MISSISSIPPI         |
|MISSOURI            |
|MONTANA             |
|NEBRASKA            |
|NEVADA              |
|NEW HAMPSHIRE       |
|NEW JERSEY          |
|NEW MEXICO          |
|NEW YORK            |
|NORTH CAROLINA      |
|NORTH DAKOTA        |
|OHIO                |
|OKLAHOMA            |
|OREGON              |
|PENNSYLVANIA        |
|RHODE ISLAND        |
|SOUTH CAROLINA      |
|SOUTH DAKOTA        |
|TENNESSEE           |
|TEXAS               |
|UTAH                |
|VERMONT             |
|VIRGINIA            |
|WASHINGTON          |
|WEST VIRGINIA       |
|WISCONSIN           |
|WYOMING             |
+--------------------+




zip,type,decommissioned,primary_city,acceptable_cities,unacceptable_cities,state,county,timezone,area_codes,world_region,country,latitude,longitude,irs_estimated_population_2015


select
    arcos.*,
    cp.*,
    zip.type,
    zip.decommissioned,
    zip.primary_city,
    zip.acceptable_cities,
    zip.unacceptable_cities,
    zip.timezone,
    zip.area_codes,
    zip.world_region,
    zip.country,
    zip.latitude,
    zip.longitude,
    zip.irs_estimated_population_2015,
    case when cp.PLACE is not null then TRUE else FALSE end as HAS_POPULATION
from arcos
    left join zip
        on arcos.BUYER_ZIP = zip.zip
    left join countyPop as cp
        on arcos.BUYER_STATE = cp.STATECODE
        and (
            arcos.BUYER_COUNTY = cp.COUNTYNAME
            or upper(zip.county) = upper(cp.NAME)
        )

select
    arcos.*,
    cp.*,
    case when cp.PLACE is not null then TRUE else FALSE end as HAS_POPULATION
from arcos
    left join countyPop as cp
        on arcos.BUYER_STATE = cp.STATECODE
        arcos.BUYER_COUNTY = cp.COUNTYNAME

select
    arcos.*,
    cp.*,
    zip.type,
    zip.decommissioned,
    zip.primary_city,
    zip.acceptable_cities,
    zip.unacceptable_cities,
    zip.timezone,
    zip.area_codes,
    zip.world_region,
    zip.country,
    zip.latitude,
    zip.longitude,
    zip.irs_estimated_population_2015,
    case when cp.PLACE is not null then TRUE else FALSE end as HAS_POPULATION
from arcos
    left join zip
        on arcos.BUYER_ZIP = zip.zip
    left join countyPop as cp
        on arcos.BUYER_STATE = cp.STATECODE
        and (
            arcos.BUYER_COUNTY = cp.COUNTYNAME
            or upper(zip.county) = upper(cp.NAME)
        )


select
    ap.*
from arcos_withCountyPop as ap
    left join zip
        on 


val data = spark.read.parquet("/tmp/data/arcos_with_pop/stage_1")
data.count
178598026
data.groupBy($"HAS_POPULATION").count.show
data.groupBy($"HAS_POPULATION").agg(sum($"QUANTITY")).show

arcos.count
178598026


time spark-submit --class io.jekal.arcos.ArcosMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar stage1
time spark-submit --class io.jekal.arcos.ArcosMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar stage2
time spark-submit --class io.jekal.arcos.ArcosModelMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar train
time spark-submit --class io.jekal.arcos.ArcosModelMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar stats
time spark-submit --class io.jekal.arcos.ArcosModelMain s3://arcos-opioid/opioids/jars/arcos_2.11.12-0.1-uber.jar report

val arcos = spark.read.option("header", true).option("sep", "\t").csv("s3://arcos-opioid/opioids/arcos_all_washpost.tsv.bz2")
val arcos_s1 = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_1")
val arcos_s2 = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/stage_2")

(arcos.count, arcos_s1.count, arcos_s2.count)

val joined = spark.read.parquet("s3://arcos-opioid/opioids/arcos_with_pop/joined")


178598026
178598026
503093578
68756857


select
    arcos.REPORTER_DEA_NO,
    arcos.REPORTER_BUS_ACT,
    arcos.REPORTER_NAME,
    arcos.REPORTER_ADDL_CO_INFO,
    arcos.REPORTER_ADDRESS1,
    arcos.REPORTER_ADDRESS2,
    arcos.REPORTER_CITY,
    arcos.REPORTER_STATE,
    arcos.REPORTER_ZIP,
    arcos.REPORTER_COUNTY,
    arcos.BUYER_DEA_NO,
    arcos.BUYER_BUS_ACT,
    arcos.BUYER_NAME,
    arcos.BUYER_ADDL_CO_INFO,
    arcos.BUYER_ADDRESS1,
    arcos.BUYER_ADDRESS2,
    arcos.BUYER_CITY,
    arcos.BUYER_STATE,
    arcos.BUYER_ZIP,
    arcos.BUYER_COUNTY,
    arcos.TRANSACTION_CODE,
    arcos.DRUG_CODE,
    arcos.NDC_NO,
    arcos.DRUG_NAME,
    arcos.QUANTITY,
    arcos.UNIT,
    arcos.ACTION_INDICATOR,
    arcos.ORDER_FORM_NO,
    arcos.CORRECTION_NO,
    arcos.STRENGTH,
    arcos.TRANSACTION_DATE,
    arcos.CALC_BASE_WT_IN_GM,
    arcos.DOSAGE_UNIT,
    arcos.TRANSACTION_ID,
    arcos.Product_Name,
    arcos.Ingredient_Name,
    arcos.Measure,
    arcos.MME_Conversion_Factor,
    arcos.Combined_Labeler_Name,
    arcos.Revised_Company_Name,
    arcos.Reporter_family,
    arcos.dos_str,
    arcos.spark_id,
    cp.SUMLEV,
    cp.STATE,
    cp.COUNTY,
    cp.PLACE,
    cp.COUSUB,
    cp.CONCIT,
    cp.NAME,
    cp.STNAME,
    cp.CENSUS2010POP,
    cp.ESTIMATESBASE2010,
    cp.POPESTIMATE2010,
    cp.POPESTIMATE2011,
    cp.POPESTIMATE2012,
    cp.COUNTYNAME,
    cp.STATECODE,
    true as HAS_POPULATION
from (
    select
        datasetA as cp,
        datasetB as arcos,
        row_number() over (partition by datasetB.spark_id order by distCol asc) as rn
    from arcos_fuzzy
) x
where
    x.rn = 1
    

select
    *
from arcos_fuzzy_populated
union
select
    *
from arcos_s1_missing
where
    not exists (select 1 from arcos_fuzzy_populated where arcos_fuzzy_populated.spark_id = arcos_s1_missing.spark_id)


select
    arcos.BUYER_DEA_NO,
    arcos.BUYER_BUS_ACT,
    arcos.BUYER_NAME,
    arcos.BUYER_ADDL_CO_INFO,
    arcos.BUYER_ADDRESS1,
    arcos.BUYER_ADDRESS2,
    arcos.BUYER_CITY,
    arcos.BUYER_STATE,
    arcos.BUYER_ZIP,
    arcos.BUYER_COUNTY,
    arcos.TRANSACTION_CODE,
    arcos.DRUG_CODE,
    arcos.NDC_NO,
    arcos.DRUG_NAME,
    arcos.QUANTITY,
    arcos.UNIT,
    arcos.ACTION_INDICATOR,
    arcos.ORDER_FORM_NO,
    arcos.CORRECTION_NO,
    arcos.STRENGTH,
    arcos.TRANSACTION_DATE,
    arcos.CALC_BASE_WT_IN_GM,
    arcos.DOSAGE_UNIT,
    arcos.TRANSACTION_ID,
    arcos.Product_Name,
    arcos.Ingredient_Name,
    arcos.Measure,
    arcos.MME_Conversion_Factor,
    arcos.Combined_Labeler_Name,
    arcos.Revised_Company_Name,
    arcos.Reporter_family,
    arcos.dos_str,
    arcos.spark_id,
    cp.SUMLEV,
    cp.STATE,
    cp.COUNTY,
    cp.PLACE,
    cp.COUSUB,
    cp.CONCIT,
    cp.NAME,
    cp.STNAME,
    cp.CENSUS2010POP,
    cp.ESTIMATESBASE2010,
    cp.POPESTIMATE2010,
    cp.POPESTIMATE2011,
    cp.POPESTIMATE2012,
    cp.COUNTYNAME,
    cp.STATECODE,
    case when cp.PLACE is not null then TRUE else FALSE end as HAS_POPULATION
from stage_2_pop_missing as arcos
    left join zip
        on arcos.BUYER_ZIP = zip.zip
    left join countyPop as cp
        on zip.state = cp.STATECODE
        and regexp_replace(zip.countyname, '\W', '') = regexp_replace(cp.COUNTYNAME, '\W', '')




+----------------------+-----------+---------------------+----------+--------------------------------+---------------------+--------------------+
|order_count_Prediction|order_count|pill_count_Prediction|pill_count|normalized_pill_count_Prediction|normalized_pill_count|            features|
+----------------------+-----------+---------------------+----------+--------------------------------+---------------------+--------------------+
|     20.95991103868321|         35|    37.90060563796545|      64.0|               382.1486530667805|                640.0|[1.0,9.0,0.0,5463...|
|     19.44831360388753|         30|    31.22757898372861|     256.0|               159.2494095814777|               1920.0|[1.0,29.0,0.0,546...|
|    21.601111440999627|         38|    37.90060563796545|      75.0|              393.84595781956983|                750.0|[1.0,38.0,0.0,546...|
|    17.798350531147133|         12|   34.682409976605285|      55.0|              154.17297280549852|                275.0|[1.0,64.0,1.0,546...|
|     19.63146950642064|         37|   30.011633902704705|      56.0|              166.87922855146965|                420.0|[1.0,77.0,1.0,546...|
+----------------------+-----------+---------------------+----------+--------------------------------+---------------------+--------------------+



Root Mean Squared Error (RMSE) on test data (order_count) = 35.1952879758064
Root Mean Squared Error (RMSE) on training data (order_count) = 35.094415107035786

Root Mean Squared Error (RMSE) on test data (pill_count) = 195.81483773100138
Root Mean Squared Error (RMSE) on training data (pill_count) = 201.82085622262446

Root Mean Squared Error (RMSE) on test data (normalized_pill_count) = 2500.4441298662223
Root Mean Squared Error (RMSE) on training data (normalized_pill_count) = 2623.3988781036774
