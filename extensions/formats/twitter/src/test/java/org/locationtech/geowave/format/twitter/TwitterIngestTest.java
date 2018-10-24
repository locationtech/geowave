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
package org.locationtech.geowave.format.twitter;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.opengis.feature.simple.SimpleFeature;

public class TwitterIngestTest
{
	private TwitterIngestPlugin ingester;
	private String filePath;
	private int expectedCount;

	@Before
	public void setup() {
		ingester = new TwitterIngestPlugin();
		ingester.init(null);

		filePath = "01234567-010101.txt.gz";
		expectedCount = 24;
	}

	@Test
	public void testIngest()
			throws IOException {

		final URL toIngest = this.getClass().getClassLoader().getResource(
				filePath);

		assertTrue(TwitterUtils.validate(toIngest));
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

			if (isValidTwitterFeature(feature)) {
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

	private boolean isValidTwitterFeature(
			final GeoWaveData<SimpleFeature> feature ) {
		if ((feature.getValue().getAttribute(
				TwitterUtils.TWITTER_TEXT_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				TwitterUtils.TWITTER_GEOMETRY_ATTRIBUTE) == null) || (feature.getValue().getAttribute(
				TwitterUtils.TWITTER_DTG_ATTRIBUTE) == null)) {
			return false;
		}
		return true;
	}
}
