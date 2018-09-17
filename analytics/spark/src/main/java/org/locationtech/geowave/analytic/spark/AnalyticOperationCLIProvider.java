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
package org.locationtech.geowave.analytic.spark;

import org.locationtech.geowave.analytic.mapreduce.operations.AnalyticSection;
import org.locationtech.geowave.analytic.spark.kmeans.operations.KmeansSparkCommand;
import org.locationtech.geowave.analytic.spark.sparksql.operations.SparkSqlCommand;
import org.locationtech.geowave.analytic.spark.spatial.operations.SpatialJoinCommand;
import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class AnalyticOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		AnalyticSection.class,
		KmeansSparkCommand.class,
		SparkSqlCommand.class,
		SpatialJoinCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
