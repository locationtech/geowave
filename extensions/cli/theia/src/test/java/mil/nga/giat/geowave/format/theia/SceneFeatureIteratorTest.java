/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import static org.hamcrest.core.Every.everyItem;
import static org.hamcrest.core.AllOf.allOf;

public class SceneFeatureIteratorTest
{
	private Matcher<SimpleFeature> hasProperties() {
		return new BaseMatcher<SimpleFeature>() {
			@Override
			public boolean matches(
					Object item ) {
				SimpleFeature feature = (SimpleFeature) item;

				return feature.getProperty("shape") != null && feature.getProperty("entityId") != null
						&& feature.getProperty("location") != null && feature.getProperty("productIdentifier") != null
						&& feature.getProperty("productType") != null && feature.getProperty("collection") != null
						&& feature.getProperty("platform") != null && feature.getProperty("processingLevel") != null
						&& feature.getProperty("startDate") != null && feature.getProperty("quicklook") != null
						&& feature.getProperty("thumbnail") != null && feature.getProperty("bands") != null
						&& feature.getProperty("resolution") != null && feature.getProperty("cloudCover") != null
						&& feature.getProperty("snowCover") != null && feature.getProperty("waterCover") != null;
			}

			@Override
			public void describeTo(
					Description description ) {
				description.appendText("feature should have properties {"
						+ "shape, entityId, location, productIdentifier, "
						+ "productType, collection, platform, processingLevel, " + "startDate, quicklook, thumbnail, "
						+ "bands, resolution, cloudCover, snowCover, waterCover" + "}");
			}
		};
	}

	private Matcher<SimpleFeature> inBounds(
			BoundingBox bounds ) {
		return new BaseMatcher<SimpleFeature>() {
			@Override
			public boolean matches(
					Object item ) {
				SimpleFeature feature = (SimpleFeature) item;
				return feature.getBounds().intersects(
						bounds);
			}

			@Override
			public void describeTo(
					Description description ) {
				description.appendText("feature should be in bounds " + bounds);
			}
		};
	}

	@Test
	public void testIterate()
			throws IOException,
			CQLException,
			ParseException,
			NoSuchAuthorityCodeException,
			FactoryException,
			MalformedURLException,
			GeneralSecurityException {

		String collection = "SENTINEL2";
		String platform = "";
		String location = "T30TWM";
		Date startDate = DateUtilities.parseISO("2018-01-28T00:00:00Z");
		Date endDate = DateUtilities.parseISO("2018-01-30T00:00:00Z");
		int orbitNumber = 0;
		int relativeOrbitNumber = 0;
		Filter cqlFilter = CQL.toFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735)");
		String workspaceDir = Tests.WORKSPACE_DIR;

		List<SimpleFeature> features = new ArrayList<>();
		try (SceneFeatureIterator iterator = new SceneFeatureIterator(
				collection,
				platform,
				location,
				startDate,
				endDate,
				orbitNumber,
				relativeOrbitNumber,
				cqlFilter,
				workspaceDir)) {
			while (iterator.hasNext()) {
				features.add(iterator.next());
			}
		}

		assertEquals(
				features.size(),
				1);
		assertThat(
				features,
				everyItem(allOf(
						hasProperties(),
						inBounds(new Envelope2D(
								new DirectPosition2D(
										-1.828,
										42.325),
								new DirectPosition2D(
										-1.624,
										42.474))))));
	}
}
