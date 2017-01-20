package mil.nga.giat.geowave.analytics.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.{ ShuffledRDD, RDD }
import org.apache.hadoop.conf.Configuration
import org.opengis.feature.simple.SimpleFeature
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey
import mil.nga.giat.geowave.analytic.partitioner.Partitioner

/**
  * Provides partitioned RDD using the provided partitioner.
  * A Partitioner can assign a SimpleFeature to more than one partition.
  */
class PartitionVectorRDD(prev: RDD[(PartitionData, SimpleFeature)])
  extends ShuffledRDD[PartitionData, SimpleFeature, SimpleFeature](prev, new GeowavePartitioner(100))

object PartitionVectorRDD {

  def apply(prev: RDD[(GeoWaveInputKey, SimpleFeature)],
            partitioner: Partitioner[SimpleFeature]): PartitionVectorRDD = {

    val pointsKeyedByBoxes = prev.mapPartitions {
      it =>
      {
        for (p <- it; r <- partitioner.getCubeIdentifiers(p._2).asScala)
          yield (r, p._2)
      }
    }
    PartitionVectorRDD(pointsKeyedByBoxes)
  }

  def apply(pointsInBoxes: RDD[(PartitionData, SimpleFeature)]) = {
    new PartitionVectorRDD(pointsInBoxes)
  }

}
