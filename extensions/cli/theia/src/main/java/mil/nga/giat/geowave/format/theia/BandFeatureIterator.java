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

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang.ArrayUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.FeatureIteratorIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.format.theia.BandFeatureIterator;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;

public class BandFeatureIterator implements
		SimpleFeatureIterator
{
	private static final String BANDS_TYPE_NAME = "theia-band";

	// List of predefined attributes
	public static final String BAND_ATTRIBUTE_NAME = "band";

	private Iterator<SimpleFeature> iterator;
	private final SceneFeatureIterator sceneIterator;

	/**
	 * Returns the SimpleFeatureTypeBuilder which provides the schema of the
	 * Bands of the Theia repository.
	 * 
	 * @return
	 * @throws NoSuchAuthorityCodeException
	 * @throws FactoryException
	 */
	public static SimpleFeatureTypeBuilder defaultBandFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException {
		SimpleFeatureTypeBuilder sceneBuilder = SceneFeatureIterator.defaultSceneFeatureTypeBuilder();

		SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.init(sceneBuilder.buildFeatureType());
		typeBuilder.setName(BANDS_TYPE_NAME);
		typeBuilder.setDefaultGeometry(SceneFeatureIterator.SHAPE_ATTRIBUTE_NAME);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				BAND_ATTRIBUTE_NAME,
				String.class);

		return typeBuilder;
	}

	public BandFeatureIterator(
			final String collection,
			final String platform,
			final String location,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber,
			final Filter cqlFilter,
			final String workspaceDir )
			throws MalformedURLException,
			IOException,
			NoSuchAuthorityCodeException,
			FactoryException,
			GeneralSecurityException {
		this(
				new SceneFeatureIterator(
						collection,
						platform,
						location,
						startDate,
						endDate,
						orbitNumber,
						relativeOrbitNumber,
						cqlFilter,
						workspaceDir),
				cqlFilter);
	}

	public BandFeatureIterator(
			final SceneFeatureIterator sceneIterator,
			final Filter cqlFilter )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		this.sceneIterator = sceneIterator;
		init(cqlFilter);
	}

	private void init(
			final Filter cqlFilter )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		final SimpleFeatureTypeBuilder typeBuilder = BandFeatureIterator.defaultBandFeatureTypeBuilder();
		final SimpleFeatureType bandType = typeBuilder.buildFeatureType();

		Iterator<SimpleFeature> featureIterator = new FeatureIteratorIterator<SimpleFeature>(
				sceneIterator);
		featureIterator = Iterators.concat(Iterators.transform(
				featureIterator,
				new SceneToBandFeatureTransform(
						bandType)));

		if (cqlFilter != null && !cqlFilter.equals(Filter.INCLUDE)) {
			final String[] attributes = DataUtilities.attributeNames(
					cqlFilter,
					bandType);

			// we can rely on the scene filtering if we don't have to check any
			// specific band filters
			if (ArrayUtils.contains(
					attributes,
					BAND_ATTRIBUTE_NAME)) {
				featureIterator = Iterators.filter(
						featureIterator,
						new CqlFilterPredicate(
								cqlFilter));
			}
		}
		iterator = featureIterator;
	}

	@Override
	public void close() {
		sceneIterator.close();
	}

	@Override
	public boolean hasNext() {
		if (iterator != null) {
			return iterator.hasNext();
		}
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		if (iterator != null) {
			return iterator.next();
		}
		return null;
	}

	private static class SceneToBandFeatureTransform implements
			Function<SimpleFeature, Iterator<SimpleFeature>>
	{
		private final SimpleFeatureBuilder featureBuilder;

		public SceneToBandFeatureTransform(
				final SimpleFeatureType type ) {
			featureBuilder = new SimpleFeatureBuilder(
					type);
		}

		@Override
		public Iterator<SimpleFeature> apply(
				SimpleFeature scene ) {
			final String entityId = scene.getID();
			final List<SimpleFeature> bands = new ArrayList<SimpleFeature>();

			for (String bandId : scene.getAttribute(
					SceneFeatureIterator.BANDS_ATTRIBUTE_NAME).toString().split(
					";")) {
				SimpleFeature band = featureBuilder.buildFeature(entityId + "_" + bandId);

				for (Property property : scene.getProperties()) {
					band.setAttribute(
							property.getName(),
							property.getValue());
				}
				band.setAttribute(
						BAND_ATTRIBUTE_NAME,
						bandId);

				bands.add(band);
			}
			return bands.iterator();
		}
	}

	private static class CqlFilterPredicate implements
			Predicate<SimpleFeature>
	{
		private final Filter cqlFilter;

		public CqlFilterPredicate(
				final Filter cqlFilter ) {
			this.cqlFilter = cqlFilter;
		}

		@Override
		public boolean apply(
				final SimpleFeature input ) {
			return cqlFilter.evaluate(input);
		}
	}
}
