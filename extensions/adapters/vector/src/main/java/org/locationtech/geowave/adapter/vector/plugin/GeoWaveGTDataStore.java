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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.geotools.data.FeatureListenerManager;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.locationtech.geowave.adapter.auth.AuthorizationSPI;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.index.IndexQueryStrategySPI;
import org.locationtech.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.plugin.lock.LockingManagement;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveAutoCommitTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionManagementState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.MemoryTransactionsAllocator;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.adapter.vector.plugin.visibility.VisibilityManagementHelper;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.visibility.VisibilityManagement;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class GeoWaveGTDataStore extends
		ContentDataStore
{
	/** Package logger */
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveGTDataStore.class);

	private FeatureListenerManager listenerManager = null;
	protected PersistentAdapterStore adapterStore;
	protected InternalAdapterStore internalAdapterStore;
	protected IndexStore indexStore;
	protected DataStatisticsStore dataStatisticsStore;
	protected DataStore dataStore;
	protected DataStoreOptions dataStoreOptions;
	protected AdapterIndexMappingStore adapterIndexMappingStore;
	private final Map<String, Index[]> preferredIndexes = new ConcurrentHashMap<>();

	private final VisibilityManagement<SimpleFeature> visibilityManagement = VisibilityManagementHelper
			.loadVisibilityManagement();
	private final AuthorizationSPI authorizationSPI;
	private final IndexQueryStrategySPI indexQueryStrategy;
	private final URI featureNameSpaceURI;
	private int transactionBufferSize = 10000;
	private final TransactionsAllocator transactionsAllocator;

	public GeoWaveGTDataStore(
			final GeoWavePluginConfig config )
			throws IOException {
		listenerManager = new FeatureListenerManager();
		lockingManager = config.getLockingManagementFactory().createLockingManager(
				config);
		authorizationSPI = config.getAuthorizationFactory().create(
				config.getAuthorizationURL());
		init(config);
		featureNameSpaceURI = config.getFeatureNamespace();
		indexQueryStrategy = config.getIndexQueryStrategy();
		transactionBufferSize = config.getTransactionBufferSize();
		transactionsAllocator = new MemoryTransactionsAllocator();
	}

	private void init(
			final GeoWavePluginConfig config ) {
		dataStore = config.getDataStore();
		dataStoreOptions = config.getDataStoreOptions();
		dataStatisticsStore = config.getDataStatisticsStore();
		indexStore = config.getIndexStore();
		adapterStore = config.getAdapterStore();
		adapterIndexMappingStore = config.getAdapterIndexMappingStore();
		internalAdapterStore = config.getInternalAdapterStore();
	}

	public AuthorizationSPI getAuthorizationSPI() {
		return authorizationSPI;
	}

	public FeatureListenerManager getListenerManager() {
		return listenerManager;
	}

	public IndexQueryStrategySPI getIndexQueryStrategy() {
		return indexQueryStrategy;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public DataStoreOptions getDataStoreOptions() {
		return dataStoreOptions;
	}

	public PersistentAdapterStore getAdapterStore() {
		return adapterStore;
	}

	public InternalAdapterStore getInternalAdapterStore() {
		return internalAdapterStore;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public DataStatisticsStore getDataStatisticsStore() {
		return dataStatisticsStore;
	}

	private Index[] filterIndices(
			Index[] unfiltered,
			boolean spatialOnly ) {
		if (spatialOnly) {
			List<Index> filtered = Lists.newArrayList();
			for (int i = 0; i < unfiltered.length; i++) {
				if (SpatialDimensionalityTypeProvider.isSpatial(unfiltered[i])) {
					filtered.add(unfiltered[i]);
				}
			}
			return filtered.toArray(new Index[filtered.size()]);
		}
		return unfiltered;
	}

	protected Index[] getIndicesForAdapter(
			final GeotoolsFeatureDataAdapter adapter,
			final boolean spatialOnly ) {
		Index[] currentSelections = preferredIndexes.get(adapter.getFeatureType().getName().toString());
		if (currentSelections != null) {
			return filterIndices(
					currentSelections,
					spatialOnly);
		}

		final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());

		final AdapterToIndexMapping adapterIndexMapping = adapterIndexMappingStore
				.getIndicesForAdapter(internalAdapterId);
		if ((adapterIndexMapping != null) && adapterIndexMapping.isNotEmpty()) {
			currentSelections = adapterIndexMapping.getIndices(indexStore);
		}
		else {
			currentSelections = getPreferredIndices(adapter);
		}
		preferredIndexes.put(
				adapter.getFeatureType().getName().toString(),
				currentSelections);
		return filterIndices(
				currentSelections,
				spatialOnly);
	}

	@Override
	public void createSchema(
			final SimpleFeatureType featureType ) {
		if (featureType.getGeometryDescriptor() == null) {
			throw new UnsupportedOperationException(
					"Schema missing geometry");
		}
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				featureType,
				visibilityManagement);
		final short adapterId = internalAdapterStore.addTypeName(adapter.getTypeName());
		if (!adapterStore.adapterExists(adapterId)) {
			// it is questionable whether createSchema *should* write the
			// adapter to the store, it is missing the proper index information
			// at this stage
			adapter.init(new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()));
			if (featureNameSpaceURI != null) {
				adapter.setNamespace(featureNameSpaceURI.toString());
			}
			final InternalDataAdapter<?> internalAdapter = new InternalDataAdapterWrapper(
					adapter,
					adapterId);
			adapterStore.addAdapter(internalAdapter);
		}
	}

	private GeotoolsFeatureDataAdapter getAdapter(
			final String typeName ) {
		final GeotoolsFeatureDataAdapter featureAdapter;
		final short adapterId = internalAdapterStore.getAdapterId(typeName);
		final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
		if ((adapter == null) || !(adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter)) {
			return null;
		}
		featureAdapter = (GeotoolsFeatureDataAdapter) adapter.getAdapter();
		if (featureNameSpaceURI != null) {
			if (adapter.getAdapter() instanceof FeatureDataAdapter) {
				((FeatureDataAdapter) featureAdapter).setNamespace(featureNameSpaceURI.toString());
			}
		}
		return featureAdapter;
	}

	@Override
	protected List<Name> createTypeNames()
			throws IOException {
		final List<Name> names = new ArrayList<>();
		final CloseableIterator<InternalDataAdapter<?>> adapters = adapterStore.getAdapters();
		while (adapters.hasNext()) {
			final InternalDataAdapter<?> adapter = adapters.next();
			if (adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter) {
				names.add(((GeotoolsFeatureDataAdapter) adapter.getAdapter()).getFeatureType().getName());
			}
		}
		adapters.close();
		return names;
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName )
			throws IOException {
		return getFeatureSource(
				typeName,
				Transaction.AUTO_COMMIT);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName,
			final Transaction tx )
			throws IOException {
		return super.getFeatureSource(
				new NameImpl(
						null,
						typeName),
				tx);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName,
			final Transaction tx )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				tx);

	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				Transaction.AUTO_COMMIT);
	}

	@Override
	protected ContentFeatureSource createFeatureSource(
			final ContentEntry entry )
			throws IOException {
		return new GeoWaveFeatureSource(
				entry,
				Query.ALL,
				getAdapter(entry.getTypeName()),
				transactionsAllocator);
	}

	@Override
	public void removeSchema(
			final Name typeName )
			throws IOException {
		this.removeSchema(typeName.getLocalPart());
	}

	@Override
	public void removeSchema(
			final String typeName )
			throws IOException {
		dataStore.removeType(typeName);
	}

	/**
	 * Used to retrieve the TransactionStateDiff for this transaction.
	 * <p>
	 *
	 * @param transaction
	 * @return GeoWaveTransactionState or null if subclass is handling
	 *         differences
	 * @throws IOException
	 */
	protected GeoWaveTransactionState getMyTransactionState(
			final Transaction transaction,
			final GeoWaveFeatureSource source )
			throws IOException {
		synchronized (transaction) {
			GeoWaveTransactionState state = null;
			if (transaction == Transaction.AUTO_COMMIT) {
				state = new GeoWaveAutoCommitTransactionState(
						source);
			}
			else {
				state = (GeoWaveTransactionState) transaction.getState(this);
				if (state == null) {
					state = new GeoWaveTransactionManagementState(
							transactionBufferSize,
							source.getComponents(),
							transaction,
							(LockingManagement) lockingManager);
					transaction.putState(
							this,
							state);
				}
			}
			return state;
		}
	}

	public Index[] getPreferredIndices(
			final GeotoolsFeatureDataAdapter adapter ) {

		final List<Index> currentSelectionsList = new ArrayList<>(
				2);
		final List<String> indexNames = SimpleFeaturePrimaryIndexConfiguration.getIndexNames(adapter.getFeatureType());
		final boolean canUseTime = adapter.hasTemporalConstraints();

		/**
		 * Requires the indices to EXIST prior to set up of the adapter.
		 * Otherwise, only Geospatial is chosen and the index Names are ignored.
		 */
		try (CloseableIterator<Index> indices = indexStore.getIndices()) {
			while (indices.hasNext()) {
				final Index index = indices.next();
				if (!indexNames.isEmpty()) {
					// Only used selected preferred indices
					if (indexNames.contains(index.getName())) {
						currentSelectionsList.add(index);
					}
				}
				@SuppressWarnings("rawtypes")
				final NumericDimensionField[] dims = index.getIndexModel().getDimensions();
				boolean hasLat = false;
				boolean hasLong = false;
				boolean hasTime = false;
				for (final NumericDimensionField<?> dim : dims) {
					hasLat |= dim instanceof LatitudeField;
					hasLong |= dim instanceof LongitudeField;
					hasTime |= dim instanceof TimeField;
				}

				if (hasLat && hasLong) {
					// If not requiring time OR (requires time AND has time
					// constraints)
					if (!hasTime || canUseTime) {
						currentSelectionsList.add(index);
					}
				}
			}
		}

		if (currentSelectionsList.isEmpty()) {
			currentSelectionsList.add(new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()));
		}

		return currentSelectionsList.toArray(new Index[currentSelectionsList.size()]);
	}
}
