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
