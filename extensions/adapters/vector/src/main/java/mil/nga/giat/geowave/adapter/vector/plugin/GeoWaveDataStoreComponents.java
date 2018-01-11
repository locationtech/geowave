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
package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class GeoWaveDataStoreComponents
{
	private final GeotoolsFeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore gtStore;
	private final TransactionsAllocator transactionAllocator;

	private final PrimaryIndex[] adapterIndices;

	public GeoWaveDataStoreComponents(
			final DataStore dataStore,
			final DataStatisticsStore dataStatisticsStore,
			final IndexStore indexStore,
			final GeotoolsFeatureDataAdapter adapter,
			final GeoWaveGTDataStore gtStore,
			final TransactionsAllocator transactionAllocator ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.indexStore = indexStore;
		this.dataStatisticsStore = dataStatisticsStore;
		this.gtStore = gtStore;
		adapterIndices = gtStore.getIndicesForAdapter(adapter);
		this.transactionAllocator = transactionAllocator;
	}

	public void initForWrite() {
		// this is ensuring the adapter is properly initialized with the
		// indicies and writing it to the adapterStore, in cases where the
		// featuredataadapter was created from geotools datastore's createSchema
		adapter.init(adapterIndices);
		gtStore.adapterStore.addAdapter(adapter);
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public GeotoolsFeatureDataAdapter getAdapter() {
		return adapter;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return gtStore;
	}

	public PrimaryIndex[] getAdapterIndices() {
		return adapterIndices;
	}

	public DataStatisticsStore getStatsStore() {
		return dataStatisticsStore;
	}

	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query ) {
		return getGTstore().getIndexQueryStrategy().getIndices(
				stats,
				query,
				gtStore.getIndicesForAdapter(adapter));
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		final QueryOptions options = new QueryOptions(
				adapter);
		options.setAuthorizations(transaction.composeAuthorizations());

		dataStore.delete(
				options,
				new DataIdQuery(
						adapter.getAdapterId(),
						adapter.getDataId(feature)));
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		final QueryOptions options = new QueryOptions(
				adapter);
		options.setAuthorizations(transaction.composeAuthorizations());

		dataStore.delete(
				options,
				new DataIdQuery(
						new ByteArrayId(
								StringUtils.stringToBinary(fid)),
						adapter.getAdapterId()));

	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Set<String> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
				new GlobalVisibilityHandler(
						transaction.composeVisibility()));

		try (IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				adapterIndices)) {
			while (featureIt.hasNext()) {
				final SimpleFeature feature = featureIt.next();
				fidList.add(feature.getID());
				indexWriter.write(
						feature,
						visibilityWriter);
			}
		}

	}

	public void writeCommit(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
				new GlobalVisibilityHandler(
						transaction.composeVisibility()));

		try (IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				adapterIndices)) {
			indexWriter.write(
					feature,
					visibilityWriter);
		}

	}

	public String getTransaction()
			throws IOException {
		return transactionAllocator.getTransaction();
	}

	public void releaseTransaction(
			final String txID )
			throws IOException {
		transactionAllocator.releaseTransaction(txID);
	}
}
