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

import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

public class PartitionParameters
{
	public enum Partition
			implements
			ParameterEnum {
		MAX_DISTANCE(
				Double.class,
				"pmd",
				"Partition Max Distance",
				false,
				true),
		PARTITION_PRECISION(
				Double.class,
				"pp",
				"Partition Precision",
				false,
				true),
		GEOMETRIC_DISTANCE_UNIT(
				String.class,
				"du",
				"Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)",
				false,
				true),
		DISTANCE_THRESHOLDS(
				String.class,
				"dt",
				"Comma separated list of distance thresholds, per dimension",
				false,
				true),
		PARTITION_DECREASE_RATE(
				Double.class,
				"pdr",
				"Rate of decrease for precision(within (0,1])",
				false,
				true),
		MAX_MEMBER_SELECTION(
				Integer.class,
				"pms",
				"Maximum number of members selected from a partition",
				false,
				true),
		SECONDARY_PARTITIONER_CLASS(
				Partitioner.class,
				"psp",
				"Perform secondary partitioning with the provided class",
				true,
				false),
		PARTITIONER_CLASS(
				Partitioner.class,
				"pc",
				"Index Identifier for Centroids",
				true,
				true);

		private final ParameterHelper<?> helper;

		private Partition(
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
