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
package mil.nga.giat.geowave.format.geolife;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import mil.nga.giat.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GEOLIFEIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private GeoLifeIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new GeoLifeIngestPlugin();
		ingester.init(null);

		filePath = "20081023025304.plt";
		expectedCount = 908;
	}

	@Test
	public void testIngest()
			throws IOException {

		final URL toIngest = this.getClass().getClassLoader().getResource(
				filePath);

		assertTrue(GeoLifeUtils.validate(toIngest));
		final Collection<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(new ByteArrayId(
				"123".getBytes(StringUtils.UTF8_CHAR_SET)));
		final CloseableIterator<GeoWaveData<SimpleFeature>> features = ingester.toGeoWaveData(
				toIngest,
				indexIds,
				"");

		assertTrue((features != null) && features.hasNext());

		int featureCount = 0;
		while (features.hasNext()) {
			final GeoWaveData<SimpleFeature> feature = features.next();

			if (isValidGeoLifeFeature(feature)) {
				featureCount++;
			}
		}
		features.close();

		final boolean readExpectedCount = (featureCount == expectedCount);
		if (!readExpectedCount) {
			System.out.println("Expected " + expectedCount + " features, ingested " + featureCount);
		}

		assertTrue(readExpectedCount);
	}

	private boolean isValidGeoLifeFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				"geometry") == null) || (feature.getValue().getAttribute(
				"trackid") == null) || (feature.getValue().getAttribute(
				"pointinstance") == null) || (feature.getValue().getAttribute(
				"Latitude") == null) || (feature.getValue().getAttribute(
				"Longitude") == null)) {
			return false;
		}
		return true;
	}

}
