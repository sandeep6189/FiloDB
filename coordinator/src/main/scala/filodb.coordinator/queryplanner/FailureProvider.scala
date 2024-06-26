package filodb.coordinator.queryplanner

import filodb.core.DatasetRef

/**
  * A provider to get failure ranges. Query engine can use failure ranges while preparing physical
  * plan to reroute or skip failure ranges. Ranges are based on dataset and over all clusters.
  * Provider will filter failure ranges by current cluster and its replicas. Failures which do not
  * belong to current cluster or its replica, will be skipped.
  */
trait FailureProvider {
  def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange]

  // The getFailures is mixing two issues:
  // (1) are we allowed to query cluster, ie whether the cluster in maintenance mode? did we have
  //     a case of invalid data being ingested and because of that we do not want to allow certain
  //     time ranges to be queried, etc
  // (2) whether the shards are up or not.
  // getMaintenancesAndDataIssues only deals with the first case
  def getMaintenancesAndDataIssues(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = ???
}

object EmptyFailureProvider extends FailureProvider {
  override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = Nil
}

/**
  * Time range.
  *
  * @param startMs epoch time in millis.
  * @param endMs   epoch time in millis.
  */
case class TimeRange(startMs: Long, endMs: Long)

/**
  * Failure details.
  *
  * @param clusterName cluster name.
  * @param datasetRef  Dataset reference for database and dataset.
  * @param timeRange   time range.
  */
case class FailureTimeRange(clusterName: String, datasetRef: DatasetRef, timeRange: TimeRange, isRemote: Boolean)

/**
  * For rerouting queries for failure ranges, Route trait will offer more context in the form of corrective
  * ranges for queries or alternative dispatchers.
  * A local route indicates a non-failure range on local cluster. A remote route indicates a non-failure
  * range on remote cluster.
  */
trait Route

case class LocalRoute(timeRange: Option[TimeRange]) extends Route

case class RemoteRoute(timeRange: Option[TimeRange]) extends Route