package mil.nga.giat.geowave.analytics.spark.tools

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import mil.nga.giat.geowave.adapter.vector.FeatureWritable
import mil.nga.giat.geowave.analytics.spark.GeoWaveRDD
import mil.nga.giat.geowave.analytics.spark.TestSuiteDataTools
import mil.nga.giat.geowave.analytic.PropertyManagement
import mil.nga.giat.geowave.analytic.param.ClusteringParameters
import mil.nga.giat.geowave.analytic.param.GlobalParameters
import mil.nga.giat.geowave.analytic.param.ExtractParameters
import mil.nga.giat.geowave.analytic.param.CommonParameters
import mil.nga.giat.geowave.analytic.model.SpatialIndexModelBuilder
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner
import org.opengis.feature.simple.SimpleFeature
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import org.apache.spark.SparkContext._

class AnalyticRecipesTest extends FlatSpec {

  val dataTool = new TestSuiteDataTools("testRDD", "geometry:Geometry:srid=4326,pid:String")
  val dataSet = dataTool.create(1000)
  val conf = new SparkConf().setAppName(
    "AnalyticRecipesTest").setMaster(
      "local");
  GeoWaveRDD.init(conf)
  val sc = new SparkContext(conf)

  def toMap(v: TraversableOnce[(SimpleFeature, SimpleFeature, Double)]) = {
    v.map(t => (t._1.getID() + t._2.getID()) -> t._3).toMap
  }

  val expectedMatches = toMap(AnalyticRecipes.compareAll(dataTool.distanceFn, 10000)(dataSet.map(t => t._2).toArray))

  "Ten distributed centroids" should "have 10 assigned distinct closest points" in {
    val rawRDD = sc.parallelize(dataSet, 5)

    val distanceFn = dataTool.distanceFn

    val centroids = dataSet.sliding(1, 100).map(x => new FeatureWritable(x.last._2.getFeatureType(), x.last._2)).toArray

    val values = AnalyticRecipes.searchDistinctKNearestNeighbor(rawRDD, distanceFn, centroids, 10);

    val mappedFeatures = values.flatMap(x => x._2);

    assert(mappedFeatures.map(c => c._2.getID()).distinct.count == 100)

    assert(values.count == 10)

    assert(values.filter(_._2.length != 10).collect.size == 0)

  }

  "Ten distributed centroids" should "have 10 assigned closest points" in {
    val rawRDD = sc.parallelize(dataSet, 5)

    val distanceFn = dataTool.distanceFn

    val centroids = dataSet.sliding(1, 100).map(x => new FeatureWritable(x.last._2.getFeatureType(), x.last._2)).toArray

    val values = AnalyticRecipes.searchKNearestNeighbor(rawRDD, distanceFn, centroids, 10);

    val mappedFeatures = values.flatMap(x => x._2);

    assert(mappedFeatures.map(c => c._2.getID()).distinct.count < 100)

    assert(mappedFeatures.count == 100)

    assert(values.count == 10)

    assert(values.filter(_._2.length != 10).collect.size == 0)

  }

  "One thousand distributed" should " have expected neighbors" in {

    val config = getConfig
    val rawRDD = sc.parallelize(dataSet, 5)
    val distanceFn = dataTool.distanceFn

    val distancePartitioner = new OrthodromicDistancePartitioner[SimpleFeature]()    
    val jobConfig = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)
    distancePartitioner.setup(config,classOf[OrthodromicDistancePartitioner[SimpleFeature]], jobConfig)
    distancePartitioner.initialize(new ScopedJobConfiguration(jobConfig,classOf[OrthodromicDistancePartitioner[SimpleFeature]]))

    val partitionedRDD = GeoWaveRDD.mapByPartition(rawRDD, distancePartitioner)

    val result = toMap(partitionedRDD.groupByKey.flatMap(AnalyticRecipes.compare(distanceFn, 10000)).distinct.collect)

    assert(result.size == expectedMatches.size)
  }

  "One thousand partitioned" should " have expected neighbors" in {
    val config = getConfig
    val rawRDD = sc.parallelize(dataSet, 5)
    val distanceFn = dataTool.distanceFn

    val partitionedRDD = GeoWaveRDD.sparkPartition(rawRDD, config, sc)

    val result = toMap(partitionedRDD.mapPartitions(AnalyticRecipes.compareByPartition(distanceFn, 10000)).distinct.collect)
    
    assert(result.size == expectedMatches.size)
  }

  def getConfig = {
    val propertyManagement = new PropertyManagement();

    propertyManagement.store(
      ClusteringParameters.Clustering.DISTANCE_THRESHOLDS.asInstanceOf[ParameterEnum[_]],
      "10,10");
    propertyManagement.store(
      CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS.asInstanceOf[ParameterEnum[_]],
      classOf[SpatialIndexModelBuilder]);
    propertyManagement.store(
      ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS.asInstanceOf[ParameterEnum[_]],
      classOf[SimpleFeatureGeometryExtractor]);
    propertyManagement.store(
      GlobalParameters.Global.CRS_ID,
      "EPSG:4326");
    propertyManagement.store(
      ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT.asInstanceOf[ParameterEnum[_]],
      "km");

    propertyManagement
  }

}