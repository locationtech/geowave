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
package mil.nga.giat.geowave.analytic;

import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A set of utilities to describe and create a simple feature for use within the
 * set of analytics.
 * 
 */
public class AnalyticFeature
{
	final static Logger LOGGER = LoggerFactory.getLogger(AnalyticFeature.class);

	public static SimpleFeature createGeometryFeature(
			final SimpleFeatureType featureType,
			final String batchId,
			final String dataId,
			final String name,
			final String groupID,
			final double weight,
			final Geometry geometry,
			final String[] extraDimensionNames,
			final double[] extraDimensions,
			final int zoomLevel,
			final int iteration,
			final long count ) {
		if (extraDimensionNames.length != extraDimensions.length) {
			LOGGER.error("The number of extraDimension names does not equal the number of extraDimensions");
			throw new IllegalArgumentException(
					"The number of extraDimension names does not equal the number of extraDimensions");
		}
		final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				featureType,
				defaults,
				dataId);
		newFeature.setAttribute(
				ClusterFeatureAttribute.NAME.attrName(),
				name);
		newFeature.setAttribute(
				ClusterFeatureAttribute.GROUP_ID.attrName(),
				groupID);
		newFeature.setAttribute(
				ClusterFeatureAttribute.ITERATION.attrName(),
				iteration);
		newFeature.setAttribute(
				ClusterFeatureAttribute.WEIGHT.attrName(),
				weight);
		newFeature.setAttribute(
				ClusterFeatureAttribute.BATCH_ID.attrName(),
				batchId);
		newFeature.setAttribute(
				ClusterFeatureAttribute.COUNT.attrName(),
				count);
		newFeature.setAttribute(
				ClusterFeatureAttribute.GEOMETRY.attrName(),
				geometry);
		newFeature.setAttribute(
				ClusterFeatureAttribute.ZOOM_LEVEL.attrName(),
				zoomLevel);
		int i = 0;
		for (final String dimName : extraDimensionNames) {
			newFeature.setAttribute(
					dimName,
					new Double(
							extraDimensions[i++]));
		}
		return newFeature;
	}

	public static FeatureDataAdapter createFeatureAdapter(
			final String centroidDataTypeId,
			final String[] extraNumericDimensions,
			final String namespaceURI,
			final String SRID,
			final ClusterFeatureAttribute[] attributes,
			final Class<? extends Geometry> geometryClass ) {
		try {
			final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			builder.setName(centroidDataTypeId);
			builder.setNamespaceURI(namespaceURI == null ? BasicFeatureTypes.DEFAULT_NAMESPACE : namespaceURI);
			builder.setSRS(SRID);
			builder.setCRS(CRS.decode(
					SRID,
					true));

			for (final ClusterFeatureAttribute attrVal : attributes) {
				builder.add(
						attrVal.name,
						attrVal.equals(ClusterFeatureAttribute.GEOMETRY) ? geometryClass : attrVal.type);
			}
			for (final String extraDim : extraNumericDimensions) {
				builder.add(
						extraDim,
						Double.class);
			}
			FeatureDataAdapter adapter = new FeatureDataAdapter(
					builder.buildFeatureType());
			// TODO any consumers of this method will not be able to utilize
			// custom CRS
			adapter.init(new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions()));
			return adapter;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Schema Creation Error.  Hint: Check the SRID.",
					e);
		}

		return null;
	}

	public static FeatureDataAdapter createGeometryFeatureAdapter(
			final String centroidDataTypeId,
			final String[] extraNumericDimensions,
			final String namespaceURI,
			final String SRID ) {
		return createFeatureAdapter(
				centroidDataTypeId,
				extraNumericDimensions,
				namespaceURI,
				SRID,
				ClusterFeatureAttribute.values(),
				Geometry.class);
	}

	public static enum ClusterFeatureAttribute {
		NAME(
				"name",
				String.class),
		GROUP_ID(
				"groupID",
				String.class),
		ITERATION(
				"iteration",
				Integer.class),
		GEOMETRY(
				"geometry",
				Geometry.class),
		WEIGHT(
				"weight",
				Double.class),
		COUNT(
				"count",
				Long.class),
		ZOOM_LEVEL(
				"level",
				Integer.class),
		BATCH_ID(
				"batchID",
				String.class);

		private final String name;
		private final Class<?> type;

		ClusterFeatureAttribute(
				final String name,
				final Class<?> type ) {
			this.name = name;
			this.type = type;
		}

		public String attrName() {
			return name;
		}

		public Class<?> getType() {
			return type;
		}
	}

}
