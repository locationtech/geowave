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
package org.locationtech.geowave.format.avro;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.opengis.feature.simple.SimpleFeature;

public class AVROIngestTest
{
	private DataSchemaOptionProvider optionsProvider;
	private AvroIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		optionsProvider = new DataSchemaOptionProvider();
		optionsProvider.setSupplementalFields(true);

		ingester = new AvroIngestPlugin();
		ingester.init(null);

		filePath = "tornado_tracksbasicIT-export.avro";
		expectedCount = 474;

	}

	@Test
	public void testIngest()
			throws IOException {

		final URL toIngest = this.getClass().getClassLoader().getResource(
				filePath);

		assertTrue(validate(toIngest));
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

			if (isValidAVROFeature(feature)) {
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

	private boolean isValidAVROFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				"the_geom") == null) || (feature.getValue().getAttribute(
				"DATE") == null) || (feature.getValue().getAttribute(
				"OM") == null) || (feature.getValue().getAttribute(
				"ELAT") == null) || (feature.getValue().getAttribute(
				"ELON") == null) || (feature.getValue().getAttribute(
				"SLAT") == null) || (feature.getValue().getAttribute(
				"SLON") == null)) {
			return false;
		}
		return true;
	}

	private boolean validate(
			final URL file ) {
		try (DataFileStream<AvroSimpleFeatureCollection> ds = new DataFileStream<>(
				file.openStream(),
				new SpecificDatumReader<AvroSimpleFeatureCollection>())) {
			if (ds.getHeader() != null) {
				return true;
			}
		}
		catch (final IOException e) {
			// Do nothing for now
		}

		return false;
	}
}
