package filodb.standalone

import com.typesafe.scalalogging.StrictLogging

import filodb.core.GlobalConfig
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.memstore.ratelimit.{CardinalityTracker, ConfigQuotaSource, RocksDbCardinalityStore}
import filodb.core.metadata.{Dataset, DatasetOptions, PartitionSchema, Schemas}

object DSCardinalityCount extends App with StrictLogging {
  start()
  // scalastyle:off method.length
  def start(): Unit = {
    try {

      //val ds_data_dir = """/tmp/ds_data"""
      val ds_data_dir = """/Users/sandeep/test_dir"""
      val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long")
      val options = DatasetOptions.DefaultOptions.copy(metricColumn = "series")
      val dataset1 = Dataset("metrics1", Seq("series:string"), columns, options)
      val schema1 = dataset1.schema.partition
      val allConfig = GlobalConfig.configToDisableAkkaCluster.withFallback(GlobalConfig.systemConfig)
      val config = allConfig.getConfig("filodb")
      val partSchema = Schemas.fromConfig(config).get.part
      val quotaSource = new ConfigQuotaSource(config, 3)
      val defaultQuota = quotaSource.getDefaults(dataset1.ref)

      val cardStoreMap = new RocksDbCardinalityStore(dataset1.ref, 10)
      val cardStoreNoMap = new RocksDbCardinalityStore(dataset1.ref, 5)

      val trackerMap = new CardinalityTracker(dataset1.ref, 10, 3,
        defaultQuota, cardStoreMap)
      quotaSource.getQuotas(dataset1.ref).foreach { q =>
        trackerMap.setQuota(q.shardKeyPrefix, q.quota)
      }

      val trackerNoMap = new CardinalityTracker(dataset1.ref, 5, 3,
        defaultQuota, cardStoreNoMap)
      quotaSource.getQuotas(dataset1.ref).foreach { q =>
        trackerNoMap.setQuota(q.shardKeyPrefix, q.quota)
      }

      println("Creating Lucene Index...")
      val startTimeMillis = System.currentTimeMillis()
      val idx = new PartKeyLuceneIndex(dataset1.ref, schema1, false,
        false, 10, 1000000,
        Some(new java.io.File(ds_data_dir)),
        None
      )

      val idx2 = new PartKeyLuceneIndex(dataset1.ref, schema1, false,
        false, 5, 1000000,
        Some(new java.io.File(ds_data_dir)),
        None
      )
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      val result = s"TimeTaken to create PartKeyLuceneIndex: $durationSeconds | Measuring cardinality now.."
      logger.info(result)
      println(result)

      println()
      println("Calculating with in-memory aggreation before writing to RocksDB..")

      for (i <- 1 to 2) {
        calculateWithMap(idx, i, partSchema, trackerMap, cardStoreMap)
      }

      println()
      println("Using existing modifyCount to write to RocksDB..")

      for (i <- 1 to 2) {
        calculateWithoutAnyMap(idx2, i, partSchema, trackerNoMap, cardStoreNoMap)
      }

    } catch {
      case e: Exception =>
        logger.error("Error occurred when initializing FiloDB server", e)
    }
  }

  def calculateWithMap(idx: PartKeyLuceneIndex, i: Int,
                       partSchema: PartitionSchema, tracker: CardinalityTracker,
                       cardStore: RocksDbCardinalityStore): Unit = {
    var startTimeMillis = System.currentTimeMillis()
    val (docsCount, totalBytes) = idx.getDownsampleCardinalityMap(partSchema, tracker)
    var endTimeMillis = System.currentTimeMillis()
    val durationSecondsCalcCard = (endTimeMillis - startTimeMillis) / 1000

    startTimeMillis = System.currentTimeMillis()
    printRocksDBScanResults(cardStore)
    endTimeMillis = System.currentTimeMillis()
    val durationSecondsScanRocksDB = (endTimeMillis - startTimeMillis) / 1000

    val totalBytesInGB = (totalBytes * 1.0) / 1000000000.0
    val result = s"Iteration: $i | DocCount: $docsCount | TotalBytes: $totalBytes " +
      s"| TotalBytesInGB: $totalBytesInGB | DurationInSecondsToCalculateCardinality: $durationSecondsCalcCard" +
      s"| DurationSecondsScanRocksDB: $durationSecondsScanRocksDB"
    logger.info(result)
    println(result)
  }

  def calculateWithoutAnyMap(idx: PartKeyLuceneIndex, i: Int,
                             partSchema: PartitionSchema, tracker: CardinalityTracker,
                             cardStore: RocksDbCardinalityStore): Unit = {
    var startTimeMillis = System.currentTimeMillis()
    val (docsCount, totalBytes) = idx.getDownsampleCardinalityNoMap(partSchema, tracker)
    var endTimeMillis = System.currentTimeMillis()
    val durationSecondsCalcCard = (endTimeMillis - startTimeMillis) / 1000

    startTimeMillis = System.currentTimeMillis()
    printRocksDBScanResults(cardStore)
    endTimeMillis = System.currentTimeMillis()
    val durationSecondsScanRocksDB = (endTimeMillis - startTimeMillis) / 1000

    val totalBytesInGB = (totalBytes*1.0)/1000000000.0
    val result = s"Iteration: $i | DocCount: $docsCount | TotalBytes: $totalBytes " +
      s"| TotalBytesInGB: $totalBytesInGB | DurationInSecondsToCalculateCardinality: $durationSecondsCalcCard" +
      s"| DurationSecondsScanRocksDB: $durationSecondsScanRocksDB"
    logger.info(result)
    println(result)
  }

  def printRocksDBScanResults(cardStore: RocksDbCardinalityStore): Unit = {
    val allWsData = cardStore.scanChildren("1")
    allWsData.foreach(c => {
      println("key: " + c.prefix.mkString(",") + " cardinality-count: " + c.value.tsCount);
    })
    println("Num Unique Workspaces: " + allWsData.size)

    val allNSData = cardStore.scanChildren("2")
    allNSData.foreach(c => {
      println("key: " + c.prefix.mkString(",") + " cardinality-count: " + c.value.tsCount);
    })
    println("Num Unique Namespaces: " + allNSData.size)

    val allMSData = cardStore.scanChildren("3")
    allMSData.foreach(c => {
      println("key: " + c.prefix.mkString(",") + " cardinality-count: " + c.value.tsCount);
    })
    println("Num Unique Metrics: " + allMSData.size)
  }
}