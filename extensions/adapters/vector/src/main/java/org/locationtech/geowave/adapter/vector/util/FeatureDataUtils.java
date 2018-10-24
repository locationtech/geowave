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
package org.locationtech.geowave.adapter.vector.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureDataUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureDataUtils.class);

	public static SimpleFeature defaultCRSTransform(
			final SimpleFeature entry,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {

		// if the feature is in a different coordinate reference system than
		// EPSG:4326, transform the geometry
		final CoordinateReferenceSystem crs = entry.getFeatureType().getCoordinateReferenceSystem();
		SimpleFeature defaultCRSEntry = entry;

		if (!GeometryUtils.getDefaultCRS().equals(
				crs)) {
			MathTransform featureTransform = null;
			if ((persistedType.getCoordinateReferenceSystem() != null)
					&& persistedType.getCoordinateReferenceSystem().equals(
							crs) && (transform != null)) {
				// we can use the transform we have already calculated for this
				// feature
				featureTransform = transform;
			}
			else if (crs != null) {
				// this feature differs from the persisted type in CRS,
				// calculate the transform
				try {
					featureTransform = CRS.findMathTransform(
							crs,
							GeometryUtils.getDefaultCRS(),
							true);
				}
				catch (final FactoryException e) {
					LOGGER
							.warn(
									"Unable to find transform to EPSG:4326, the feature geometry will remain in its original CRS",
									e);
				}
			}
			if (featureTransform != null) {
				defaultCRSEntry = GeometryUtils.crsTransform(
						defaultCRSEntry,
						reprojectedType,
						featureTransform);
			}
		}
		return defaultCRSEntry;
	}

	public static String getAxis(
			final CoordinateReferenceSystem crs ) {
		// Some geometries do not have a CRS provided. Thus we default to
		// urn:ogc:def:crs:EPSG::4326
		final CoordinateSystem cs = crs == null ? null : crs.getCoordinateSystem();
		if ((cs != null) && (cs.getDimension() > 0)) {
			return cs.getAxis(
					0).getDirection().name().toString();
		}
		return "EAST";
	}

	public static SimpleFeatureType decodeType(
			final String nameSpace,
			final String typeName,
			final String typeDescriptor,
			final String axis )
			throws SchemaException {

		SimpleFeatureType featureType = (nameSpace != null) && (nameSpace.length() > 0) ? DataUtilities.createType(
				nameSpace,
				typeName,
				typeDescriptor) : DataUtilities.createType(
				typeName,
				typeDescriptor);

		final String lCaseAxis = axis.toLowerCase(Locale.ENGLISH);
		final CoordinateReferenceSystem crs = featureType.getCoordinateReferenceSystem();
		final String typeAxis = getAxis(crs);
		// Default for EPSG:4326 is lat/long, If the provided type was
		// long/lat, then re-establish the order
		if ((crs != null) && crs.getIdentifiers().toString().contains(
				"EPSG:4326") && !lCaseAxis.equalsIgnoreCase(typeAxis)) {
			final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			builder.init(featureType);

			try {
				// truely no way to force lat first
				// but it is the default in later versions of GeoTools.
				// this all depends on the authority at the time of creation
				featureType = SimpleFeatureTypeBuilder.retype(
						featureType,
						CRS.decode(
								"EPSG:4326",
								lCaseAxis.equals("east")));
			}
			catch (final FactoryException e) {
				throw new SchemaException(
						"Cannot decode EPSG:4326",
						e);
			}
		}
		return featureType;

	}

	public static SimpleFeature buildFeature(
			final SimpleFeatureType featureType,
			final Pair<String, Object>[] entries ) {

		final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}
		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				featureType,
				defaults,
				UUID.randomUUID().toString());
		for (final Pair<String, Object> entry : entries) {
			newFeature.setAttribute(
					entry.getKey(),
					entry.getValue());
		}
		return newFeature;
	}

	public static SimpleFeatureType getFeatureType(
			final DataStorePluginOptions dataStore,
			String typeName ) {
		// if no id provided, locate a single featureadapter
		if (typeName == null) {
			final List<String> typeNameList = FeatureDataUtils.getFeatureTypeNames(dataStore);
			if (typeNameList.size() >= 1) {
				typeName = typeNameList.get(0);
			}
			else if (typeNameList.isEmpty()) {
				LOGGER.error("No feature adapters found for use with time param");

				return null;
			}
			else {
				LOGGER.error("Multiple feature adapters found. Please specify one.");

				return null;
			}
		}

		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();

		final DataTypeAdapter<?> adapter = adapterStore.getAdapter(
				internalAdapterStore.getAdapterId(typeName)).getAdapter();

		if ((adapter != null) && (adapter instanceof GeotoolsFeatureDataAdapter)) {
			final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
			return gtAdapter.getFeatureType();
		}

		return null;
	}

	public static FeatureDataAdapter cloneFeatureDataAdapter(
			final DataStorePluginOptions storeOptions,
			final String originalTypeName,
			final String newTypeName ) {

		// Get original feature type info
		final SimpleFeatureType oldType = FeatureDataUtils.getFeatureType(
				storeOptions,
				originalTypeName);

		// Build type using new name
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		sftBuilder.init(oldType);
		sftBuilder.setName(newTypeName);
		final SimpleFeatureType newType = sftBuilder.buildFeatureType();

		// Create new adapter that will use new typename
		final FeatureDataAdapter newAdapter = new FeatureDataAdapter(
				newType);

		return newAdapter;
	}

	public static String getGeomField(
			final DataStorePluginOptions dataStore,
			final String typeName ) {
		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();

		final DataTypeAdapter<?> adapter = adapterStore.getAdapter(
				internalAdapterStore.getAdapterId(typeName)).getAdapter();

		if ((adapter != null) && (adapter instanceof GeotoolsFeatureDataAdapter)) {
			final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
			final SimpleFeatureType featureType = gtAdapter.getFeatureType();

			if (featureType.getGeometryDescriptor() != null) {
				return featureType.getGeometryDescriptor().getLocalName();
			}
		}

		return null;
	}

	public static String getTimeField(
			final DataStorePluginOptions dataStore,
			final String typeName ) {
		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();

		final DataTypeAdapter<?> adapter = adapterStore.getAdapter(
				internalAdapterStore.getAdapterId(typeName)).getAdapter();

		if ((adapter != null) && (adapter instanceof GeotoolsFeatureDataAdapter)) {
			final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
			final SimpleFeatureType featureType = gtAdapter.getFeatureType();
			final TimeDescriptors timeDescriptors = gtAdapter.getTimeDescriptors();

			// If not indexed, try to find a time field
			if ((timeDescriptors == null) || !timeDescriptors.hasTime()) {
				for (final AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
					final Class<?> bindingClass = attrDesc.getType().getBinding();
					if (TimeUtils.isTemporal(bindingClass)) {
						return attrDesc.getLocalName();
					}
				}
			}
			else {
				if (timeDescriptors.getTime() != null) {
					return timeDescriptors.getTime().getLocalName();
				}
				else if (timeDescriptors.getStartRange() != null) {
					// give back start|stop string
					return timeDescriptors.getStartRange().getLocalName() + "|"
							+ timeDescriptors.getEndRange().getLocalName();
				}
			}
		}

		return null;
	}

	public static int getFeatureAdapterCount(
			final DataStorePluginOptions dataStore ) {
		try (final CloseableIterator<InternalDataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters()) {

			int featureAdapters = 0;

			while (adapterIt.hasNext()) {
				final DataTypeAdapter<?> adapter = adapterIt.next().getAdapter();
				if (adapter instanceof GeotoolsFeatureDataAdapter) {
					featureAdapters++;
				}
			}

			return featureAdapters;
		}
	}

	public static List<String> getFeatureTypeNames(
			final DataStorePluginOptions dataStore ) {
		final ArrayList<String> featureTypeNames = new ArrayList<>();

		try (final CloseableIterator<InternalDataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters()) {
			while (adapterIt.hasNext()) {
				final DataTypeAdapter<?> adapter = adapterIt.next().getAdapter();
				if (adapter instanceof GeotoolsFeatureDataAdapter) {
					featureTypeNames.add(adapter.getTypeName());
				}
			}

			return featureTypeNames;
		}
	}
}
