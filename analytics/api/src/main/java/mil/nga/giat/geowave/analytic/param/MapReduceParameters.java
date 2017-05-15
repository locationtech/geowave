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

import java.util.Arrays;
import java.util.Collection;

public class MapReduceParameters
{

	public enum MRConfig
			implements
			ParameterEnum {
		CONFIG_FILE(
				String.class,
				"conf",
				"MapReduce Configuration",
				true),
		HDFS_HOST_PORT(
				String.class,
				"hdfs",
				"HDFS hostname and port in the format hostname:port",
				true),
		HDFS_BASE_DIR(
				String.class,
				"hdfsbase",
				"Fully qualified path to the base directory in hdfs",
				true),
		YARN_RESOURCE_MANAGER(
				String.class,
				"resourceman",
				"Yarn resource manager hostname and port in the format hostname:port",
				true),
		JOBTRACKER_HOST_PORT(
				String.class,
				"jobtracker",
				"Hadoop job tracker hostname and port in the format hostname:port",
				true);

		private final ParameterHelper<?> helper;

		private MRConfig(
				final Class baseClass,
				final String name,
				final String description,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					false,
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

	public static final Collection<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			MRConfig.CONFIG_FILE,
			MRConfig.HDFS_BASE_DIR,
			MRConfig.HDFS_HOST_PORT,
			MRConfig.JOBTRACKER_HOST_PORT,
			MRConfig.YARN_RESOURCE_MANAGER
		});
	}
}
