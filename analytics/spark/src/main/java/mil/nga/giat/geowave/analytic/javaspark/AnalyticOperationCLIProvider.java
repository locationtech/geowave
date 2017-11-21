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
package mil.nga.giat.geowave.analytic.javaspark;

import mil.nga.giat.geowave.analytic.javaspark.kmeans.operations.KmeansSparkCommand;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.operations.SparkSqlCommand;
import mil.nga.giat.geowave.analytic.mapreduce.operations.AnalyticSection;
import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class AnalyticOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		AnalyticSection.class,
		KmeansSparkCommand.class,
		SparkSqlCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}
