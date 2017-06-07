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
package mil.nga.giat.geowave.analytic.param;

public class ClusteringParameters
{

	public enum Clustering
			implements
			ParameterEnum {
		MAX_REDUCER_COUNT(
				Integer.class,
				"crc",
				"Maximum Clustering Reducer Count",
				false,
				true),
		RETAIN_GROUP_ASSIGNMENTS(
				Boolean.class,
				"ga",
				"Retain Group assignments during execution",
				false,
				false),
		MINIMUM_SIZE(
				Integer.class,
				"cms",
				"Minimum Cluster Size",
				false,
				true),
		MAX_ITERATIONS(
				Integer.class,
				"cmi",
				"Maximum number of iterations when finding optimal clusters",
				false,
				true),
		CONVERGANCE_TOLERANCE(
				Double.class,
				"cct",
				"Convergence Tolerance",
				false,
				true),
		ZOOM_LEVELS(
				Integer.class,
				"zl",
				"Number of Zoom Levels to Process",
				false,
				true);

		private final ParameterHelper<?> helper;

		private Clustering(
				final Class baseClass,
				final String name,
				final String description,
				final boolean isClass,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					isClass,
					hasArg);
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper<?> getHelper() {
			return helper;
		}
	}

}
