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
package org.locationtech.geowave.analytic;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.opengis.feature.simple.SimpleFeatureType;

public class SerializableAdapterStoreTest
{
	@Test
	public void testSerialization()
			throws ClassNotFoundException,
			IOException {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);
		adapter.init(index);
		final SerializableAdapterStore store = new SerializableAdapterStore(
				new MemoryAdapterStore(
						new DataTypeAdapter<?>[] {
							adapter
						}));

		final String id = "centroid";
		assertNotNull(checkSerialization(
				store).getAdapter(
				id));
	}

	private SerializableAdapterStore checkSerialization(
			final SerializableAdapterStore store )
			throws IOException,
			ClassNotFoundException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (ObjectOutputStream os = new ObjectOutputStream(
				bos)) {
			os.writeObject(store);
			os.flush();
		}
		final ByteArrayInputStream bis = new ByteArrayInputStream(
				bos.toByteArray());
		try (ObjectInputStream is = new ObjectInputStream(
				bis)) {
			return (SerializableAdapterStore) is.readObject();
		}
	}
}
