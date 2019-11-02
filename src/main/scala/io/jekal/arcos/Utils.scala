package io.jekal.arcos

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

object Utils {
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
}
