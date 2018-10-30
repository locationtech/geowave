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
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.adapter.vector.index.IndexQueryStrategySPI.QueryHint;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UniformVisibilityWriter;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.opengis.feature.simple.SimpleFeature;
import org.spark_project.guava.collect.Maps;

public class GeoWaveDataStoreComponents
{
	private final GeotoolsFeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore gtStore;
	private final TransactionsAllocator transactionAllocator;

	private final Index[] adapterIndices;

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
		adapterIndices = gtStore.getIndicesForAdapter(
				adapter,
				false);
		this.transactionAllocator = transactionAllocator;
	}

	public void initForWrite() {
		// this is ensuring the adapter is properly initialized with the
		// indicies and writing it to the adapterStore, in cases where the
		// featuredataadapter was created from geotools datastore's createSchema
		if (adapter instanceof InitializeWithIndicesDataAdapter) {
			((InitializeWithIndicesDataAdapter) adapter).init(adapterIndices);
		}
		final short internalAdapterId = gtStore.getInternalAdapterStore().getAdapterId(
				adapter.getTypeName());
		final InternalDataAdapter<?> internalDataAdapter = new InternalDataAdapterWrapper(
				adapter,
				internalAdapterId);
		gtStore.adapterStore.addAdapter(internalDataAdapter);
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

	public Index[] getAdapterIndices() {
		return adapterIndices;
	}

	public DataStatisticsStore getStatsStore() {
		return dataStatisticsStore;
	}

	public CloseableIterator<Index> getIndices(
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats,
			final BasicQuery query,
			final boolean spatialOnly ) {
		final GeoWaveGTDataStore gtStore = getGTstore();
		final Map<QueryHint, Object> queryHints = Maps.newHashMap();
		queryHints.put(
				QueryHint.MAX_RANGE_DECOMPOSITION,
				gtStore.getDataStoreOptions().getMaxRangeDecomposition());
		Index[] indices = gtStore.getIndicesForAdapter(
				adapter,
				spatialOnly);
		if (spatialOnly && indices.length == 0) {
			throw new UnsupportedOperationException(
					"Query required spatial index, but none were found.");
		}
		return gtStore.getIndexQueryStrategy().getIndices(
				stats,
				query,
				indices,
				queryHints);
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();

		dataStore.delete(bldr.setAuthorizations(
				transaction.composeAuthorizations()).addTypeName(
				adapter.getTypeName()).constraints(
				bldr.constraintsFactory().dataIds(
						new ByteArray[] {
							adapter.getDataId(feature)
						})).build());
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();

		dataStore.delete(bldr.setAuthorizations(
				transaction.composeAuthorizations()).addTypeName(
				adapter.getTypeName()).constraints(
				bldr.constraintsFactory().dataIds(
						new ByteArray[] {
							new ByteArray(
									StringUtils.stringToBinary(fid))
						})).build());

	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Set<String> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<>(
				new GlobalVisibilityHandler<>(
						transaction.composeVisibility()));
		dataStore.addType(
				adapter,
				adapterIndices);
		try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
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

		final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<>(
				new GlobalVisibilityHandler<>(
						transaction.composeVisibility()));
		dataStore.addType(
				adapter,
				adapterIndices);
		try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
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
