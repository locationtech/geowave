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
package mil.nga.giat.geowave.adapter.vector.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.geotools.data.DataUtilities;
import org.geotools.factory.GeoTools;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.TimeUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

public class FeatureDataUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureDataUtils.class);
	private static final Object MUTEX = new Object();
	private static boolean classLoaderInitialized = false;

	public static void initClassLoader()
			throws MalformedURLException {
		synchronized (MUTEX) {
			if (classLoaderInitialized) {
				return;
			}
			final ClassLoader classLoader = FeatureDataUtils.class.getClassLoader();
			LOGGER.info("Generating patched classloader");
			if (classLoader instanceof VFSClassLoader) {
				final VFSClassLoader cl = (VFSClassLoader) classLoader;
				final FileObject[] fileObjs = cl.getFileObjects();
				final URL[] fileUrls = new URL[fileObjs.length];
				for (int i = 0; i < fileObjs.length; i++) {
					fileUrls[i] = new URL(
							fileObjs[i].toString());
				}
				final ClassLoader urlCL = java.security.AccessController
						.doPrivileged(new java.security.PrivilegedAction<URLClassLoader>() {
							@Override
							public URLClassLoader run() {
								final URLClassLoader ucl = new URLClassLoader(
										fileUrls,
										cl);
								return ucl;
							}
						});
				GeoTools.addClassLoader(urlCL);

			}
			classLoaderInitialized = true;
		}
	}

	public static SimpleFeature crsTransform(
			final SimpleFeature entry,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		SimpleFeature crsEntry = entry;

		if (transform != null) {
			// we can use the transform we have already calculated for this
			// feature
			try {

				// this will clone the feature and retype it to Index CRS
				crsEntry = SimpleFeatureBuilder.retype(
						entry,
						reprojectedType);

				// this will transform the geometry
				crsEntry.setDefaultGeometry(JTS.transform(
						(Geometry) entry.getDefaultGeometry(),
						transform));
			}
			catch (MismatchedDimensionException | TransformException e) {
				LOGGER
						.warn(
								"Unable to perform transform to specified CRS of the index, the feature geometry will remain in its original CRS",
								e);
			}
		}

		return crsEntry;
	}

	public static SimpleFeature defaultCRSTransform(
			final SimpleFeature entry,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {

		// if the feature is in a different coordinate reference system than
		// EPSG:4326, transform the geometry
		final CoordinateReferenceSystem crs = entry.getFeatureType().getCoordinateReferenceSystem();
		SimpleFeature defaultCRSEntry = entry;

		if (!GeometryUtils.DEFAULT_CRS.equals(crs)) {
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
							GeometryUtils.DEFAULT_CRS,
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
				defaultCRSEntry = crsTransform(
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
			ByteArrayId adapterId ) {
		// if no id provided, locate a single featureadapter
		if (adapterId == null) {
			final List<ByteArrayId> adapterIdList = FeatureDataUtils.getFeatureAdapterIds(dataStore);
			if (adapterIdList.size() == 1) {
				adapterId = adapterIdList.get(0);
			}
			else if (adapterIdList.isEmpty()) {
				LOGGER.error("No feature adapters found for use with time param");

				return null;
			}
			else {
				LOGGER.error("Multiple feature adapters found. Please specify one.");

				return null;
			}
		}

		final AdapterStore adapterStore = dataStore.createAdapterStore();

		final DataAdapter adapter = adapterStore.getAdapter(adapterId);

		if ((adapter != null) && (adapter instanceof GeotoolsFeatureDataAdapter)) {
			final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
			return gtAdapter.getFeatureType();
		}

		return null;
	}

	public static String getGeomField(
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		final AdapterStore adapterStore = dataStore.createAdapterStore();

		final DataAdapter adapter = adapterStore.getAdapter(adapterId);

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
			final ByteArrayId adapterId ) {
		final AdapterStore adapterStore = dataStore.createAdapterStore();

		final DataAdapter adapter = adapterStore.getAdapter(adapterId);

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
		final CloseableIterator<DataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters();
		int featureAdapters = 0;

		while (adapterIt.hasNext()) {
			final DataAdapter adapter = adapterIt.next();
			if (adapter instanceof GeotoolsFeatureDataAdapter) {
				featureAdapters++;
			}
		}

		return featureAdapters;
	}

	public static List<ByteArrayId> getFeatureAdapterIds(
			final DataStorePluginOptions dataStore ) {
		final ArrayList<ByteArrayId> featureAdapterIds = new ArrayList<>();

		final CloseableIterator<DataAdapter<?>> adapterIt = dataStore.createAdapterStore().getAdapters();

		while (adapterIt.hasNext()) {
			final DataAdapter adapter = adapterIt.next();
			if (adapter instanceof GeotoolsFeatureDataAdapter) {
				featureAdapterIds.add(adapter.getAdapterId());
			}
		}

		return featureAdapterIds;
	}
}
