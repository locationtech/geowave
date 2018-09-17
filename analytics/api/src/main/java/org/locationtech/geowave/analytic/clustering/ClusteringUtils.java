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
package org.locationtech.geowave.analytic.clustering;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.geotools.feature.type.BasicFeatureTypes;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.extract.DimensionExtractor;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.index.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Polygon;

public class ClusteringUtils
{

	public static final String CLUSTERING_CRS = "EPSG:4326";

	final static Logger LOGGER = LoggerFactory.getLogger(ClusteringUtils.class);

	private static DataAdapter<?> createAdapter(
			final String sampleDataTypeId,
			final String sampleDataNamespaceURI,
			final AdapterStore adapterStore,
			final String[] dimensionNames ) {

		final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				sampleDataTypeId,
				dimensionNames,
				sampleDataNamespaceURI,
				CLUSTERING_CRS);

		final ByteArrayId dbId = new ByteArrayId(
				sampleDataTypeId);
		if (!adapterStore.adapterExists(dbId)) {
			adapterStore.addAdapter(adapter);
			return adapter;
		}
		else {
			return adapterStore.getAdapter(dbId);
		}

	}

	public static DataAdapter[] getAdapters(
			final PropertyManagement propertyManagement )
			throws IOException {

		PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				propertyManagement);

		final AdapterStore adapterStore = store.getDataStoreOptions().createAdapterStore();

		final org.locationtech.geowave.core.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
		final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
		while (it.hasNext()) {
			adapters.add(it.next());
		}
		it.close();
		final DataAdapter[] result = new DataAdapter[adapters.size()];
		adapters.toArray(result);
		return result;
	}

	public static PrimaryIndex[] getIndices(
			final PropertyManagement propertyManagement ) {

		PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				propertyManagement);

		final IndexStore indexStore = store.getDataStoreOptions().createIndexStore();

		final org.locationtech.geowave.core.store.CloseableIterator<Index<?, ?>> it = indexStore.getIndices();
		final List<PrimaryIndex> indices = new LinkedList<PrimaryIndex>();
		while (it.hasNext()) {
			indices.add((PrimaryIndex) it.next());
		}
		try {
			it.close();
		}
		catch (final IOException e) {
			LOGGER.warn("Unable to close iterator" + e);
		}
		final PrimaryIndex[] result = new PrimaryIndex[indices.size()];
		indices.toArray(result);
		return result;
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static QueryRanges getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final QueryRanges ranges = DataStoreUtils.constraintsToQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index),
				index.getIndexStrategy(),
				-1);

		return ranges;
	}

	public static PrimaryIndex createIndex(
			final PropertyManagement propertyManagement ) {

		PersistableStore store = (PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
				propertyManagement);

		final IndexStore indexStore = store.getDataStoreOptions().createIndexStore();
		return (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID)));
	}

	public static DataAdapter<?> createAdapter(
			final PropertyManagement propertyManagement )
			throws ClassNotFoundException,
			InstantiationException,
			IllegalAccessException {

		final Class<DimensionExtractor> dimensionExtractorClass = propertyManagement.getPropertyAsClass(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				DimensionExtractor.class);

		return ClusteringUtils.createAdapter(
				propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
				propertyManagement.getPropertyAsString(
						CentroidParameters.Centroid.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				((PersistableStore) StoreParameters.StoreParam.INPUT_STORE.getHelper().getValue(
						propertyManagement)).getDataStoreOptions().createAdapterStore(),
				dimensionExtractorClass.newInstance().getDimensionNames());
	}
}
