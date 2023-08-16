package filodb.standalone

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.memstore.ratelimit.{CardinalityTracker, RocksDbCardinalityStore}
import filodb.core.metadata.{Dataset, DatasetOptions, PartitionSchema}

object DSCardPerfTest extends App with StrictLogging {

  val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long")
  val options = DatasetOptions.DefaultOptions.copy(metricColumn = "series")
  val dataset1 = Dataset("metrics1", Seq("series:string"), columns, options)
  val schema1 = dataset1.schema.partition

  start()

  def runOnlyLuceneIterateTest(idx: PartKeyLuceneIndex, iterationLimit: Int): Unit = {

    println("Running iteration over lucene index test for path: " + idx.indexDiskLocation.toAbsolutePath.toString)

//    val startTimeMillis = System.currentTimeMillis()
//    val idx = new PartKeyLuceneIndex(dataset1.ref, schema1, false, false, 10, 1000000,
//      Some(new java.io.File(idxPath)), None)
//    val endTimeMillis = System.currentTimeMillis()
//    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
//    val result = s"TimeTaken to create PartKeyLuceneIndex: $durationSeconds | Iterating through docs now ..."
//    logger.info(result)
//    println(result)

    for (i <- 1 to iterationLimit) {
      iterateLuceneIndex(idx, i)
    }
  }

  def runLuceneIterateAndRocksDBStoreTest(idx: PartKeyLuceneIndex, iterationLimit: Int,
                                          gflushCount: Option[Int]): Unit = {

    val idxPath = idx.indexDiskLocation.toAbsolutePath.toString()
    println("Iteration over lucene index + rocks db store store for path: " + idxPath + " flush: " + gflushCount)
    logger.info("Iteration over lucene index + rocks db store store for path: " + idxPath + " flush: " + gflushCount)

    val rocksDBStore = new RocksDbCardinalityStore(dataset1.ref, 10)
    val cardTracker = new CardinalityTracker(dataset1.ref, 10, 3,
      Seq(2000000000, 2000000000, 2000000000, 2000000000), rocksDBStore, flushCount = gflushCount)

//    val endTimeMillis = System.currentTimeMillis()
//    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
//    val result = s"TimeTaken to create PartKeyLuceneIndex + CardTracker: $durationSeconds "
//    logger.info(result)
//    println(result)

    for (i <- 1 to iterationLimit) {
      iterateAndStore(idx, i, schema1, cardTracker)
    }
  }

  def start(): Unit = {
    try {

//      val old_idx = new PartKeyLuceneIndex(dataset1.ref, schema1, false, false, 10, 100000000,
//        Some(new java.io.File("/opt/ds_data_old/sh_10_idx")), None)

//      val old_idx_temp = new PartKeyLuceneIndex(dataset1.ref, schema1, false, false, 10, 1000000,
//        Some(new java.io.File("/tmp/ds_data_old")), None)

      val new_idx = new PartKeyLuceneIndex(dataset1.ref, schema1, false, false, 10, 100000000,
        Some(new java.io.File("/opt/ds_data_new/new_sh_10_idx")), None)

//      val new_idx_temp = new PartKeyLuceneIndex(dataset1.ref, schema1, false, false, 10, 1000000,
//        Some(new java.io.File("/tmp/ds_data_new")), None)

      //runOnlyLuceneIterateTest(old_idx, 5)
      //runOnlyLuceneIterateTest(old_idx_temp, 5)
      //runOnlyLuceneIterateTest(new_idx, 5)
      //runOnlyLuceneIterateTest(new_idx_temp, 5)
      //runOnlyLuceneIterateTest("/opt/ds_data_new/new_sh_10_idx", 5)
      //runLuceneIterateAndRocksDBStoreTest(old_idx, 1, Some(1000000))
      runLuceneIterateAndRocksDBStoreTest(new_idx, 1, Some(1000000))

      //runLuceneIterateAndRocksDBStoreTest(old_idx, 1, Some(100000))
      runLuceneIterateAndRocksDBStoreTest(new_idx, 1, Some(100000))

      //runLuceneIterateAndRocksDBStoreTest(old_idx_temp, 1, Some(1000000))
      //runLuceneIterateAndRocksDBStoreTest(new_idx_temp, 1, Some(1000000))
    } catch {
      case e: Exception =>
        logger.error("Error occurred when initializing FiloDB server", e)
        println("Error occurred when initializing FiloDB server: " + e.toString())
    }
  }

  def iterateLuceneIndex(idx: PartKeyLuceneIndex, i: Int): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    val (docsCount, totalBytes) = idx.getAllDocsCount()
    val totalBytesInGB = (totalBytes * 1.0) / 1000000000.0
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    val result = s"Iteration: $i | DocCount: $docsCount | TotalBytes: $totalBytes " +
      s"| TotalBytesInGB: $totalBytesInGB |DurationInSeconds: $durationSeconds"
    logger.info(result)
    println(result)
  }

  def iterateAndStore(idx: PartKeyLuceneIndex, i: Int, partSchema: PartitionSchema,
                      cardTracker: CardinalityTracker): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    idx.calculateCardinality(partSchema, cardTracker)
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    val result = s"Iteration: $i |DurationInSeconds: $durationSeconds"
    logger.info(result)
    println(result)
  }
}