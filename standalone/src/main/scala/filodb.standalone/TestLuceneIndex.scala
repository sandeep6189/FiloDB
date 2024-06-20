package filodb.standalone

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.metadata.{Dataset, DatasetOptions}

object TestLuceneIndex extends App with StrictLogging {

  start()
  def start(): Unit = {
    try {

      val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long")
      val options = DatasetOptions.DefaultOptions.copy(metricColumn = "series")
      val dataset1 = Dataset("metrics1", Seq("series:string"), columns, options)
      val schema1 = dataset1.schema.partition

      println("Running lucene-test from /opt/ds_data folder (part of docker image)")
      val startTimeMillis = System.currentTimeMillis()
      val idx = new PartKeyLuceneIndex(dataset1.ref, schema1, false,
        false, 10, 1000000,
        Some(new java.io.File("/opt/ds_data")),
        None
      )
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      val result = s"TimeTaken to create PartKeyLuceneIndex: $durationSeconds | Iterating through docs now ..."
      logger.info(result)
      println(result)

      for (i <- 1 to 5) {
        iterateLuceneIndex(idx, i)
      }

      println()
      println("Running lucene-test from /tmp/ds_data folder!")
      val startTimeMillis2 = System.currentTimeMillis()
      val idx2 = new PartKeyLuceneIndex(dataset1.ref, schema1, false,
        false, 10, 1000000,
        Some(new java.io.File("/tmp/ds_data")),
        None
      )
      val endTimeMillis2 = System.currentTimeMillis()
      val durationSeconds2 = (endTimeMillis2 - startTimeMillis2) / 1000
      val result2 = s"TimeTaken to create PartKeyLuceneIndex: $durationSeconds2 | Iterating through docs now ..."
      logger.info(result2)
      println(result2)

      for (i2 <- 1 to 5) {
        iterateLuceneIndex(idx2, i2)
      }

    } catch {
      case e: Exception =>
        logger.error("Error occurred when initializing FiloDB server", e)
    }
  }

  def iterateLuceneIndex(idx: PartKeyLuceneIndex, i: Int): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    val (docsCount, totalBytes) = idx.getAllDocsCount()
    val totalBytesInGB = (totalBytes*1.0)/1000000000.0
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    val result = s"Iteration: $i | DocCount: $docsCount | TotalBytes: $totalBytes " +
      s"| TotalBytesInGB: $totalBytesInGB |DurationInSeconds: $durationSeconds"
    logger.info(result)
    println(result)
  }
}
