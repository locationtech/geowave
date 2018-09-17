/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.operations.remote.options;

import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions.PartitionStrategy;

import com.beust.jcommander.Parameter;

public class BasicIndexOptions
{

	@Parameter(names = {
		"--indexName"
	}, description = "A custom name can be given to this index. Default name will be the based on configuration parameters.")
	protected String nameOverride = null;

	@Parameter(names = {
		"-np",
		"--numPartitions"
	}, description = "The number of partitions.  Default partitions will be 1.")
	protected int numPartitions = 1;

	@Parameter(names = {
		"-ps",
		"--partitionStrategy"
	}, description = "The partition strategy to use.  Default will be none.")
	protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

	public String getNameOverride() {
		return nameOverride;
	}

	public void setNameOverride(
			String nameOverride ) {
		this.nameOverride = nameOverride;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(
			int numPartitions ) {
		this.numPartitions = numPartitions;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
	}

	public void setPartitionStrategy(
			PartitionStrategy partitionStrategy ) {
		this.partitionStrategy = partitionStrategy;
	}

}
