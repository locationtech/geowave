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
package org.locationtech.geowave.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;

/**
 * This class provides utility methods for accessing job context configuration
 * parameters that are specific to the GeoWaveInputFormat.
 */
public class GeoWaveInputConfigurator extends
		GeoWaveConfiguratorBase
{
	protected static enum InputConfig {
		QUERY_CONSTRAINTS,
		INDEX_QUERY_OPTIONS,
		DATA_TYPE_QUERY_OPTIONS,
		COMMON_QUERY_OPTIONS,
		MIN_SPLITS,
		MAX_SPLITS,
		OUTPUT_WRITABLE, // used to inform the input format to output a Writable
							// from the HadoopDataAdapter
		AUTHORIZATION
	}

	private static QueryConstraints getQueryConstraintsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.QUERY_CONSTRAINTS),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return (QueryConstraints) PersistenceUtils.fromBinary(queryBytes);
		}
		return null;
	}

	private static IndexQueryOptions getIndexQueryOptionsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.INDEX_QUERY_OPTIONS),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return (IndexQueryOptions) PersistenceUtils.fromBinary(queryBytes);
		}
		return null;
	}

	private static DataTypeQueryOptions<?> getDataTypeQueryOptionsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.DATA_TYPE_QUERY_OPTIONS),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return (DataTypeQueryOptions<?>) PersistenceUtils.fromBinary(queryBytes);
		}
		return null;
	}

	private static CommonQueryOptions getCommonQueryOptionsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.COMMON_QUERY_OPTIONS),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return (CommonQueryOptions) PersistenceUtils.fromBinary(queryBytes);
		}
		return null;
	}

	private static Integer getMinimumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MIN_SPLITS);
	}

	private static Integer getMaximumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MAX_SPLITS);
	}

	private static Integer getIntegerConfigInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final InputConfig inputConfig ) {
		final String str = configuration.get(
				enumToConfKey(
						implementingClass,
						inputConfig),
				"");
		if ((str != null) && !str.isEmpty()) {
			final Integer retVal = Integer.parseInt(str);
			return retVal;
		}
		return null;
	}

	public static Index getIndex(
			final Class<?> implementingClass,
			final Configuration config ) {
		final String input = config.get(enumToConfKey(
				implementingClass,
				GeoWaveConfg.INDEX));
		if (input != null) {
			final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(input);
			return (Index) PersistenceUtils.fromBinary(indexBytes);
		}
		return null;
	}

	public static QueryConstraints getQueryCosntraints(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getQueryConstraintsInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setQueryConstraints(
			final Class<?> implementingClass,
			final Configuration config,
			final QueryConstraints query ) {
		if (query != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.QUERY_CONSTRAINTS),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(query)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.QUERY_CONSTRAINTS));
		}
	}

	public static IndexQueryOptions getIndexQueryOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getIndexQueryOptionsInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setIndexQueryOptions(
			final Class<?> implementingClass,
			final Configuration config,
			final IndexQueryOptions queryOptions ) {
		if (queryOptions != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.INDEX_QUERY_OPTIONS),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(queryOptions)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.INDEX_QUERY_OPTIONS));
		}
	}

	public static DataTypeQueryOptions<?> getDataTypeQueryOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getDataTypeQueryOptionsInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setDataTypeQueryOptions(
			final Class<?> implementingClass,
			final Configuration config,
			final DataTypeQueryOptions<?> queryOptions ) {
		if (queryOptions != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.DATA_TYPE_QUERY_OPTIONS),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(queryOptions)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.DATA_TYPE_QUERY_OPTIONS));
		}
	}

	public static CommonQueryOptions getCommonQueryOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getCommonQueryOptionsInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setCommonQueryOptions(
			final Class<?> implementingClass,
			final Configuration config,
			final CommonQueryOptions queryOptions ) {
		if (queryOptions != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.COMMON_QUERY_OPTIONS),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(queryOptions)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.COMMON_QUERY_OPTIONS));
		}
	}

	public static Integer getMinimumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMinimumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setMinimumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer minSplits ) {
		if (minSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MIN_SPLITS),
					minSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MIN_SPLITS));
		}
	}

	public static Integer getMaximumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMaximumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setMaximumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer maxSplits ) {
		if (maxSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MAX_SPLITS),
					maxSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MAX_SPLITS));
		}
	}
}
