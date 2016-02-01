package mil.nga.giat.geowave.analytics.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.opengis.feature.simple.SimpleFeature
import org.apache.spark.SparkContext
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey
import mil.nga.giat.geowave.core.store.query.Query
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import mil.nga.giat.geowave.analytic.partitioner.Partitioner
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData
import scala.collection.JavaConverters._
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner
import mil.nga.giat.geowave.core.store.adapter.DataAdapter
import mil.nga.giat.geowave.core.store.index.PrimaryIndex
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.PairFunction
import mil.nga.giat.geowave.core.store.query.DistributableQuery
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter
import org.geotools.feature.simple.SimpleFeatureBuilder
import mil.nga.giat.geowave.analytic.PropertyManagement


/**
  * Convenience obejct to provide different RDDs.
  *
  */
object GeoWaveRDD {

  def init(conf: SparkConf): SparkConf = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    conf.set(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer");
    conf.set(
      "spark.kryo.registrator",
      classOf[GeoWaveKryoRegistrator].getCanonicalName());
  }

  /**
    * Example of forming an RDD using the GeoWavInputFormat
    *
    * minSplits and maxSplits should be used to get optimal splits for the data.
    * The default splits are necessarily optimal
    */
  def rddForSimpleFeatures(sc: SparkContext,
                           index: PrimaryIndex,
                           adapter: DataAdapter[SimpleFeature],
                           minSplits: Int,
                           maxSplits: Int,
                           query: DistributableQuery)(implicit geoWaveContext: GeoWaveContext): RDD[(GeoWaveInputKey, SimpleFeature)] = {

    val conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)

    GeoWaveInputFormat.setDataStoreName(conf,
      geoWaveContext.dataStoreName)

    GeoWaveInputFormat.setStoreConfigOptions(conf,
      geoWaveContext.storeParameters)

    GeoWaveInputFormat.setGeoWaveNamespace(conf,
      geoWaveContext.tableNameSpace)

    // index and adapters are not mandatory.
    // they are used here as an example
    GeoWaveInputFormat.addIndex(conf, index)
    GeoWaveInputFormat.addDataAdapter(conf, adapter)

    // query is not mandatory.
    GeoWaveInputFormat.setQuery(conf, query)

    // recommended
    GeoWaveInputFormat.setMaximumSplitCount(conf, maxSplits)
    GeoWaveInputFormat.setMinimumSplitCount(conf, minSplits)

    sc.newAPIHadoopRDD(conf, classOf[GeoWaveInputFormat[SimpleFeature]], classOf[GeoWaveInputKey], classOf[SimpleFeature])
  }

  /**
    * Translate a set of objects in a JavaRDD to SimpleFeatures and push to GeoWave
    */
  def writeFeatureToGeoWave[V](sc: SparkContext,
                               index: PrimaryIndex,
                               adapter: FeatureDataAdapter,
                               inputRDD: JavaRDD[V],
                               toOutput: (V) => SimpleFeature)(implicit geoWaveContext: GeoWaveContext) = {

    writeToGeoWave(sc, index, adapter, inputRDD, toOutput)
  }

  /**
    * Translate a set of objects in a JavaRDD to a provided type and push to GeoWave
    */
  def writeToGeoWave[V, OutputType](sc: SparkContext,
                                    index: PrimaryIndex,
                                    adapter: DataAdapter[OutputType],
                                    inputRDD: JavaRDD[V],
                                    toOutput: V => OutputType)(implicit geoWaveContext: GeoWaveContext) = {

    //setup the configuration and the output format
    val conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)

    GeoWaveOutputFormat.setDataStoreName(conf,
      geoWaveContext.dataStoreName)

    GeoWaveOutputFormat.setStoreConfigOptions(conf,
      geoWaveContext.storeParameters)

    GeoWaveOutputFormat.setGeoWaveNamespace(conf,
      geoWaveContext.tableNameSpace)

    GeoWaveOutputFormat.addIndex(conf, index)
    GeoWaveOutputFormat.addDataAdapter(conf, adapter)

    //create the job
    val job = new org.apache.hadoop.mapreduce.Job(conf)
    job.setOutputKeyClass(classOf[GeoWaveOutputKey])
    job.setOutputValueClass(classOf[SimpleFeature])
    job.setOutputFormatClass(classOf[GeoWaveOutputFormat])

    // broadcast byte ids
    val adapterId = sc.broadcast(adapter.getAdapterId())
    val indexId = sc.broadcast(index.getId())

    //map to a pair containing the output key and the output value
    inputRDD.mapToPair(new PairFunction[V, GeoWaveOutputKey, OutputType] {
      override def call(inputValue: V): (GeoWaveOutputKey, OutputType) = {
        (new GeoWaveOutputKey(adapterId.value, indexId.value), toOutput(inputValue))
      }
    }) saveAsNewAPIHadoopDataset (job.getConfiguration)
  }

  def sparkPartition(rdd: RDD[(GeoWaveInputKey, SimpleFeature)], pm: PropertyManagement, sc: SparkContext): PartitionVectorRDD = {
    val distancePartitioner = new OrthodromicDistancePartitioner[SimpleFeature]();
    val jobConfig = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)
    distancePartitioner.setup(pm,classOf[OrthodromicDistancePartitioner[SimpleFeature]], jobConfig)
    distancePartitioner.initialize(new ScopedJobConfiguration(jobConfig,classOf[OrthodromicDistancePartitioner[SimpleFeature]]));
    PartitionVectorRDD(rdd, distancePartitioner)
  }

  def mapByPartition(rdd: RDD[(GeoWaveInputKey, SimpleFeature)],
                     partitioner: Partitioner[SimpleFeature]): RDD[(PartitionData, SimpleFeature)] = {
    rdd.flatMap(kv => { partitioner.getCubeIdentifiers(kv._2).asScala.map(pd => (pd, kv._2)) })
  }

}
