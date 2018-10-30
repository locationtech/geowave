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
package org.locationtech.geowave.mapreduce;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can run a basic job to query GeoWave. It manages datastore
 * connection params, adapters, indices, query, min splits and max splits.
 */
public abstract class AbstractGeoWaveJobRunner extends
		Configured implements
		Tool
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveJobRunner.class);

	protected DataStorePluginOptions dataStoreOptions;
	protected QueryConstraints constraints = null;
	protected CommonQueryOptions commonOptions;
	protected DataTypeQueryOptions<?> dataTypeOptions;
	protected IndexQueryOptions indexOptions;
	protected Integer minInputSplits = null;
	protected Integer maxInputSplits = null;

	public AbstractGeoWaveJobRunner(
			final DataStorePluginOptions dataStoreOptions ) {
		this.dataStoreOptions = dataStoreOptions;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws Exception {
		final Job job = Job.getInstance(super.getConf());
		// must use the assembled job configuration
		final Configuration conf = job.getConfiguration();

		GeoWaveInputFormat.setStoreOptions(
				conf,
				dataStoreOptions);

		GeoWaveOutputFormat.setStoreOptions(
				conf,
				dataStoreOptions);

		job.setJarByClass(this.getClass());

		configure(job);

		if (commonOptions != null) {
			GeoWaveInputFormat.setCommonQueryOptions(
					conf,
					commonOptions);

		}
		if (dataTypeOptions != null) {
			GeoWaveInputFormat.setDataTypeQueryOptions(
					conf,
					dataTypeOptions,
					dataStoreOptions.createAdapterStore(),
					dataStoreOptions.createInternalAdapterStore());

		}
		if (indexOptions != null) {
			GeoWaveInputFormat.setIndexQueryOptions(
					conf,
					indexOptions,
					dataStoreOptions.createIndexStore());

		}
		if (constraints != null) {
			GeoWaveInputFormat.setQueryConstraints(
					conf,
					constraints);
		}
		if (minInputSplits != null) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					minInputSplits);
		}
		if (maxInputSplits != null) {
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					maxInputSplits);
		}

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	protected abstract void configure(
			Job job )
			throws Exception;

	public void setMaxInputSplits(
			final int maxInputSplits ) {
		this.maxInputSplits = maxInputSplits;
	}

	public void setMinInputSplits(
			final int minInputSplits ) {
		this.minInputSplits = minInputSplits;
	}

	public void setQuery(
			Query<?> query ) {
		setCommonQueryOptions(query.getCommonQueryOptions());
		setDataTypeQueryOptions(query.getDataTypeQueryOptions());
		setIndexQueryOptions(query.getIndexQueryOptions());
		setQueryConstraints(query.getQueryConstraints());
	}

	public void setCommonQueryOptions(
			final CommonQueryOptions commonOptions ) {
		this.commonOptions = commonOptions;
	}

	public void setDataTypeQueryOptions(
			final DataTypeQueryOptions<?> dataTypeOptions ) {
		this.dataTypeOptions = dataTypeOptions;
	}

	public void setIndexQueryOptions(
			final IndexQueryOptions indexOptions ) {
		this.indexOptions = indexOptions;
	}

	public void setQueryConstraints(
			final QueryConstraints constraints ) {
		this.constraints = constraints;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		return runOperation(args) ? 0 : -1;
	}

	public boolean runOperation(
			final String[] args )
			throws ParseException {
		try {
			return runJob() == 0 ? true : false;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run job",
					e);
			throw new ParseException(
					e.getMessage());
		}
	}

}
