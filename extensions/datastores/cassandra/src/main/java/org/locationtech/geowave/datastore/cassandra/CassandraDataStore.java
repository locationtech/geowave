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
package org.locationtech.geowave.datastore.cassandra;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.operations.CassandraOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class CassandraDataStore extends
		BaseMapReduceDataStore
{
	public CassandraDataStore(
			final CassandraOperations operations,
			final CassandraOptions options ) {
		super(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new SecondaryIndexStoreImpl(),
				operations,
				options,
				new InternalAdapterStoreImpl(
						operations));
	}

	@Override
	public void prepareRecordWriter(
			final Configuration conf ) {
		// because datastax cassandra driver requires guava 19.0, this user
		// classpath must override the default hadoop classpath which has an old
		// version of guava or there will be incompatibility issues
		conf.setBoolean(
				MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
				true);
	}

	@Override
	public List<InputSplit> getSplits(
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final QueryConstraints constraints,
			final TransientAdapterStore adapterStore,
			final AdapterIndexMappingStore aimStore,
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore,
			final IndexStore indexStore,
			final JobContext context,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		context.getConfiguration().setBoolean(
				MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
				true);
		return super.getSplits(
				commonOptions,
				typeOptions,
				indexOptions,
				constraints,
				adapterStore,
				aimStore,
				statsStore,
				internalAdapterStore,
				indexStore,
				context,
				minSplits,
				maxSplits);
	}
}
