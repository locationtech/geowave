package mil.nga.giat.geowave.analytics.spark

import org.apache.spark.rdd.RDD
import mil.nga.giat.geowave.core.index.ByteArrayId
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData


class GeowavePartitioner(private val size: Int)
  extends org.apache.spark.Partitioner
  with Serializable {

  override def numPartitions: Int = size

  override def getPartition(key: Any): Int = {
    key match {
      case (b1: ByteArrayId) => b1.hashCode % size
      case (pd: PartitionData) => pd.hashCode % size
      case _ => 0 // Throw an exception?
    }
  }

}