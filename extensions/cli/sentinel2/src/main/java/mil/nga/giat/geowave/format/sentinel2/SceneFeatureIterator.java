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
package mil.nga.giat.geowave.format.sentinel2;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

public class SceneFeatureIterator implements
		SimpleFeatureIterator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SceneFeatureIterator.class);

	private static final String SCENES_DIR = "scenes";

	// List of predefined attributes.
	public static final String SHAPE_ATTRIBUTE_NAME = "shape";
	public static final String ENTITY_ID_ATTRIBUTE_NAME = "entityId";
	public static final String PROVIDER_NAME_ATTRIBUTE_NAME = "provider";
	public static final String LOCATION_ATTRIBUTE_NAME = "location";
	public static final String PRODUCT_ID_ATTRIBUTE_NAME = "productIdentifier";
	public static final String PRODUCT_TYPE_ATTRIBUTE_NAME = "productType";
	public static final String COLLECTION_ATTRIBUTE_NAME = "collection";
	public static final String PLATFORM_ATTRIBUTE_NAME = "platform";
	public static final String PROCESSING_LEVEL_ATTRIBUTE_NAME = "processingLevel";
	public static final String ACQUISITION_DATE_ATTRIBUTE_NAME = "startDate";
	public static final String QUICKLOOK_ATTRIBUTE_NAME = "quicklook";
	public static final String THUMBNAIL_ATTRIBUTE_NAME = "thumbnail";
	public static final String BANDS_ATTRIBUTE_NAME = "bands";
	public static final String RESOLUTION_ATTRIBUTE_NAME = "resolution";
	public static final String CLOUD_COVER_ATTRIBUTE_NAME = "cloudCover";
	public static final String SNOW_COVER_ATTRIBUTE_NAME = "snowCover";
	public static final String WATER_COVER_ATTRIBUTE_NAME = "waterCover";
	public static final String ORBIT_NUMBER_ATTRIBUTE_NAME = "orbitNumber";
	public static final String RELATIVE_ORBIT_NUMBER_ATTRIBUTE_NAME = "relativeOrbitNumber";
	public static final String SCENE_DOWNLOAD_ATTRIBUTE_NAME = "sceneDownloadUrl";

	private Sentinel2ImageryProvider provider;
	private Iterator<SimpleFeature> iterator;
	private SimpleFeatureType type;

	/**
	 * Default SimpleFeatureTypeBuilder which provides the Scene schema of a
	 * Sentinel2 repository.
	 *
	 * @return
	 * @throws NoSuchAuthorityCodeException
	 * @throws FactoryException
	 */
	public static SimpleFeatureTypeBuilder defaultSceneFeatureTypeBuilder(
			String typeName )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(typeName);
		typeBuilder.setCRS(CRS.decode(
				"EPSG:4326",
				true));
		typeBuilder.setDefaultGeometry(SHAPE_ATTRIBUTE_NAME);

		// shape (Geometry),
		// entityId (String), provider (String), location (String),
		// productIdentifier (String),
		// productType (String), collection (String), platform (String),
		// processingLevel (String), startDate (Date), quicklook (String),
		// thumbnail (String),
		// bands (String), resolution (Integer),
		// cloudCover (Integer), snowCover (Integer), waterCover (Integer),
		// orbitNumber (Integer), relativeOrbitNumber (Integer),
		// and the feature ID is entityId for the scene
		//
		typeBuilder.add(
				SHAPE_ATTRIBUTE_NAME,
				Polygon.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				ENTITY_ID_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PROVIDER_NAME_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				LOCATION_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PRODUCT_ID_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PRODUCT_TYPE_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				COLLECTION_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PLATFORM_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				PROCESSING_LEVEL_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				ACQUISITION_DATE_ATTRIBUTE_NAME,
				Date.class);
		typeBuilder.add(
				QUICKLOOK_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				THUMBNAIL_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				BANDS_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				RESOLUTION_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				CLOUD_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				SNOW_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				WATER_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				ORBIT_NUMBER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				RELATIVE_ORBIT_NUMBER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				SCENE_DOWNLOAD_ATTRIBUTE_NAME,
				String.class);

		return typeBuilder;
	}

	public SceneFeatureIterator(
			final String providerName,
			final String collection,
			final String platform,
			final String location,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber,
			final Filter cqlFilter,
			final String workspaceDir )
			throws NoSuchAuthorityCodeException,
			FactoryException,
			MalformedURLException,
			IOException,
			GeneralSecurityException {
		init(
				new File(
						workspaceDir,
						SCENES_DIR),
				providerName,
				collection,
				platform,
				location,
				startDate,
				endDate,
				orbitNumber,
				relativeOrbitNumber,
				cqlFilter);
	}

	private void init(
			final File scenesDir,
			final String providerName,
			final String collection,
			final String platform,
			final String location,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber,
			final Filter cqlFilter )
			throws NoSuchAuthorityCodeException,
			FactoryException,
			IOException,
			GeneralSecurityException {

		if (!scenesDir.exists() && !scenesDir.mkdirs()) {
			LOGGER.warn("Unable to create directory '" + scenesDir.getAbsolutePath() + "'");
		}

		// Get the Sentinel2 provider.
		provider = Sentinel2ImageryProvider.getProvider(providerName);
		if (provider == null) {
			throw new RuntimeException(
					"Unable to find '" + providerName + "' Sentinel2 provider");
		}

		// Split out the spatial part of the filter.
		Envelope envelope = null;
		if ((cqlFilter != null) && !cqlFilter.equals(Filter.INCLUDE)) {
			Envelope bounds = new Envelope();
			bounds = (Envelope) cqlFilter.accept(
					ExtractBoundsFilterVisitor.BOUNDS_VISITOR,
					bounds);

			if ((bounds != null) && !bounds.isNull() && !bounds.equals(infinity())) {
				envelope = bounds;
			}
		}

		final SimpleFeatureTypeBuilder typeBuilder = provider.sceneFeatureTypeBuilder();
		type = typeBuilder.buildFeatureType();

		// Fetch the meta data of found Sentinel2 products.
		Iterator<SimpleFeature> featureIterator = provider.searchScenes(
				scenesDir,
				collection,
				platform,
				location,
				envelope,
				startDate,
				endDate,
				orbitNumber,
				relativeOrbitNumber);

		if ((featureIterator != null) && (cqlFilter != null) && !cqlFilter.equals(Filter.INCLUDE)) {
			Filter actualFilter;

			if (hasOtherProperties(cqlFilter)) {
				final List<AttributeDescriptor> descriptorList = type.getAttributeDescriptors();

				final String[] propertyNames = new String[descriptorList.size()];
				for (int i = 0, icount = descriptorList.size(); i < icount; i++) {
					propertyNames[i] = descriptorList.get(
							i).getLocalName();
				}

				final PropertyIgnoringFilterVisitor visitor = new PropertyIgnoringFilterVisitor(
						propertyNames,
						type);
				actualFilter = (Filter) cqlFilter.accept(
						visitor,
						null);
			}
			else {
				actualFilter = cqlFilter;
			}

			final CqlFilterPredicate filterPredicate = new CqlFilterPredicate(
					actualFilter);
			featureIterator = Iterators.filter(
					featureIterator,
					filterPredicate);
		}
		iterator = featureIterator;
	}

	private boolean hasOtherProperties(
			final Filter cqlFilter ) {
		final String[] attributes = DataUtilities.attributeNames(
				cqlFilter,
				type);

		for (final String attribute : attributes) {
			if (type.getDescriptor(attribute) == null) {
				return true;
			}
		}
		return false;
	}

	public SimpleFeatureType getFeatureType() {
		return type;
	}

	public Sentinel2ImageryProvider getProvider() {
		return provider;
	}

	@Override
	public void close() {}

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

	private Envelope infinity() {
		return new Envelope(
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY,
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY);
	}

	public static class CqlFilterPredicate implements
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
