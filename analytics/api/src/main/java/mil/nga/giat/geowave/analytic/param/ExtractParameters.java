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

import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class ExtractParameters
{
	public enum Extract
			implements
			ParameterEnum {
		OUTPUT_DATA_TYPE_ID(
				String.class,
				"eot",
				"Output Data Type ID",
				false,
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"ens",
				"Output Data Namespace URI",
				false,
				true),
		REDUCER_COUNT(
				Integer.class,
				"erc",
				"Number of Reducers For initial data extraction and de-duplication",
				false,
				true),
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class,
				"ede",
				"Class to extract dimensions into a simple feature output",
				true,
				true),
		QUERY(
				DistributableQuery.class,
				"eq",
				"Query",
				false,
				true),
		QUERY_OPTIONS(
				QueryOptions.class,
				"eqf",
				"Restricted extracted field list (comma-separated list of field ids)",
				false,
				true),
		MAX_INPUT_SPLIT(
				Integer.class,
				"emx",
				"Maximum input split size",
				false,
				true),
		MIN_INPUT_SPLIT(
				Integer.class,
				"emn",
				"Minimum input split size",
				false,
				true),
		GROUP_ID(
				String.class,
				"eg",
				"Group ID assigned to extracted data",
				false,
				true);

		private transient final ParameterHelper<?> helper;

		private Extract(
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
