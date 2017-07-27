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

import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum<Object> {
		REDUCER_COUNT(
				Integer.class,
				"orc",
				"Number of Reducers For Output",
				false,
				true),
		OUTPUT_FORMAT(
				FormatConfiguration.class,
				"ofc",
				"Output Format Class",
				true,
				true),
		INDEX_ID(
				String.class,
				"oid",
				"Output Index ID for objects that will be written to GeoWave",
				false,
				true),
		DATA_TYPE_ID(
				String.class,
				"odt",
				"Output Data ID assigned to objects that will be written to GeoWave",
				false,
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"ons",
				"Output namespace for objects that will be written to GeoWave",
				false,
				true),
		HDFS_OUTPUT_PATH(
				Path.class,
				"oop",
				"Output HDFS File Path",
				false,
				true);
		private final ParameterHelper<Object> helper;

		private Output(
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
		public ParameterHelper<Object> getHelper() {
			return helper;
		}
	}
}
