package filodb.jmh

import java.util.concurrent.TimeUnit

import org.agrona.concurrent.UnsafeBuffer
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.memory.NativeMemoryManager
import filodb.memory.format._
import filodb.memory.format.vectors.{BinaryHistogram, HistogramVector}

@State(Scope.Thread)
class HistVectorBenchmark {
  import vectors._

  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)
  final val numDataPoints = 60
  val bucketScheme = Base2ExpHistogramBuckets(3, -78, 126)
  val counts = Array.fill(bucketScheme.numBuckets)(5L)
  val buffer = new UnsafeBuffer(new Array[Byte](4096))

  val appender = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
  val appender2 = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
  (0 until numDataPoints).foreach { i =>
    val hist = LongHistogram(bucketScheme, counts)
    hist.serialize(Some(buffer))
    if (appender2.addData(buffer) != Ack) {
      throw new RuntimeException(s"Failed to add histogram $i")
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(numDataPoints)
  def ingestion(): Unit = {
    (0 until numDataPoints).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (appender.addData(buffer) != Ack) {
        throw new RuntimeException(s"Failed to add histogram $i")
      }
    }
    appender.reset()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def queries(blackhole: Blackhole): Unit = {
    val r2 = appender2.reader.asHistReader
    val len = r2.length(MemoryAccessor.nativePtrAccessor, appender.addr)
    (0 until len).foreach { i =>
      val h = r2(i)
      blackhole.consume(h)
    }
  }

  val appenderExp = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
  val appenderExp2 = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
  (0 until numDataPoints).foreach { i =>
    val hist = LongHistogram(bucketScheme, counts)
    hist.serialize(Some(buffer))
    if (appenderExp2.addData(buffer) != Ack) {
      throw new RuntimeException(s"Failed to add histogram $i")
    }
  }


  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(numDataPoints)
  def ingestionExpVector(): Unit = {
    (0 until 60).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (appenderExp.addData(buffer) != Ack) {
        throw new RuntimeException(s"Failed to add histogram $i")
      }
    }
    appenderExp.reset()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def queriesExpVector(blackhole: Blackhole): Unit = {
    val r2 = appenderExp2.reader.asHistReader
    val len = r2.length(MemoryAccessor.nativePtrAccessor, appenderExp2.addr)
    (0 until len).foreach { i =>
      val h = r2(i)
      blackhole.consume(h)
    }
  }

  // ── sum() benchmark: simulates rate(delta_hist[5m]) hot path ───────
  // Parameterized by bucket count. 30 samples = 5m window @ 10s interval.
  // Runs both optimized (DeltaHistogramReader) and baseline (RowHistogramReader) paths.
  //
  // Run all:  sbt "jmh/jmh:run -i 5 -wi 3 -f 1 filodb.jmh.HistDeltaSumBenchmark"
  // Run one:  sbt "jmh/jmh:run -i 5 -wi 3 -f 1 -p numBuckets=20 filodb.jmh.HistDeltaSumBenchmark"
}

@State(Scope.Thread)
class HistDeltaSumBenchmark {
  import vectors._

  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)
  val buffer = new UnsafeBuffer(new Array[Byte](4096))
  final val numSamplesInWindow = 30

  @Param(Array("5", "10", "15", "20", "30", "127"))
  var numBuckets: Int = 20

  var appender: BinaryAppendableVector[org.agrona.DirectBuffer] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val scheme = if (numBuckets == 127) Base2ExpHistogramBuckets(3, -78, 126)
                 else GeometricBuckets(1.0, 2.0, numBuckets)
    appender = HistogramVector.appending(memFactory, 15000)
    val rng = new java.util.Random(42)
    val bucketCount = scheme.numBuckets
    (0 until numSamplesInWindow).foreach { _ =>
      val raw = new Array[Long](bucketCount)
      (0 until bucketCount).foreach { i => raw(i) = rng.nextInt(1000).toLong }
      (1 until bucketCount).foreach { i => raw(i) += raw(i - 1) }
      BinaryHistogram.writeDelta(scheme, raw, buffer)
      if (appender.addData(buffer) != Ack) {
        throw new RuntimeException(s"Failed to add $bucketCount-bucket histogram")
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumOptimized(bh: Blackhole): Unit = {
    HistogramVector.optimizedDeltaSumEnabled = true
    bh.consume(appender.reader.asHistReader.sum(0, numSamplesInWindow - 1))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumBaseline(bh: Blackhole): Unit = {
    HistogramVector.optimizedDeltaSumEnabled = false
    bh.consume(appender.reader.asHistReader.sum(0, numSamplesInWindow - 1))
  }
}
