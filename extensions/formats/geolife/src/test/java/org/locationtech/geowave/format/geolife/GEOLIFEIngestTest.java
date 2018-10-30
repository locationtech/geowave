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
package org.locationtech.geowave.format.geolife;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
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
		final CloseableIterator<GeoWaveData<SimpleFeature>> features = ingester.toGeoWaveData(
				toIngest,
				new String[] {
					"123"
				},
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
