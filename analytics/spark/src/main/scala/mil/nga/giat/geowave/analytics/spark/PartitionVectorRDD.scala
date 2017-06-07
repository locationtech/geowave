/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
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
