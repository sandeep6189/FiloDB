package filodb.query.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
class HierarchicalQueryExperienceSpec extends AnyFunSpec with Matchers {

  it("getMetricColumnFilterTag should return expected column") {
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "__name__"), "_metric_") shouldEqual "__name__"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "_metric_"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "__name__") shouldEqual "__name__"
  }

  it("getNextLevelAggregatedMetricName should return expected metric name") {

    val params = IncludeAggRule(":::", "agg_2", Set("job", "instance"))

    // Case 1: Should not update if metric doesn't have the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName("__name__", params,
      Seq(ColumnFilter("__name__", Equals("metric1")), ColumnFilter("job", Equals("spark")))) shouldEqual Some("metric1")

    // Case 2: Should update if metric has the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName("__name__", params,
      Seq(ColumnFilter("__name__", Equals("metric1:::agg")), ColumnFilter("job", Equals("spark")))) shouldEqual
      Some("metric1:::agg_2")

    // Case 3: Should not update if metricColumnFilter and column filters don't match
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName("_metric_", params,
      Seq(ColumnFilter("__name__", Equals("metric1:::agg")), ColumnFilter("job", Equals("spark")))) shouldEqual
      None

    // Case 4: Similar to case 1 but with a different metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName("_metric_", params,
      Seq(ColumnFilter("_metric_", Equals("metric1:::agg")), ColumnFilter("job", Equals("spark")))) shouldEqual
      Some("metric1:::agg_2")
  }

  it("isParentPeriodicSeriesPlanAllowedForRawSeriesUpdateForHigherLevelAggregatedMetric return expected values") {
    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "Aggregate", "ScalarOperation")) shouldEqual true

    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "ScalarOperation")) shouldEqual false
  }

  it("isRangeFunctionAllowed should return expected values") {
    HierarchicalQueryExperience.isRangeFunctionAllowed("rate") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("increase") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("sum_over_time") shouldEqual false
    HierarchicalQueryExperience.isRangeFunctionAllowed("last") shouldEqual false
  }

  it("isAggregationOperatorAllowed should return expected values") {
    HierarchicalQueryExperience.isAggregationOperatorAllowed("sum") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("min") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("max") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("avg") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("count") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("topk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("bottomk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stddev") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stdvar") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("quantile") shouldEqual false
  }

  it("should check if higher level aggregation is applicable with IncludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule(":::", "agg_2", Set("tag1", "tag2")), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule(":::", "agg_2", Set("tag1", "tag2", "tag3")), Seq("tag1", "tag2", "_ws_", "_ns_", "__name__")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule(":::", "agg_2", Set("tag1", "tag2", "tag3")), Seq("tag3", "tag4", "_ws_", "_ns_", "__name__")) shouldEqual false
  }

  it("should check if higher level aggregation is applicable with ExcludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule(":::", "agg_2", Set("tag1", "tag2")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule(":::", "agg_2", Set("tag1", "tag3")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule(":::", "agg_2", Set("tag1", "tag2")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule(":::", "agg_2", Set("tag3", "tag4")), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true
  }
}
