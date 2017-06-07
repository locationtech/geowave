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

import java.io.IOException;
import java.util.UUID;

import mil.nga.giat.geowave.analytic.AnalyticFeature.ClusterFeatureAttribute;

import org.apache.hadoop.mapreduce.JobContext;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class SimpleFeatureItemWrapperFactory implements
		AnalyticItemWrapperFactory<SimpleFeature>
{

	@Override
	public AnalyticItemWrapper<SimpleFeature> create(
			final SimpleFeature item ) {
		return new SimpleFeatureAnalyticItemWrapper(
				item);
	}

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws IOException {}

	public static class SimpleFeatureAnalyticItemWrapper implements
			AnalyticItemWrapper<SimpleFeature>
	{

		final SimpleFeature item;

		public SimpleFeatureAnalyticItemWrapper(
				final SimpleFeature item ) {
			this.item = item;
		}

		@Override
		public String getID() {
			return item.getID();
		}

		@Override
		public SimpleFeature getWrappedItem() {
			return item;
		}

		@Override
		public long getAssociationCount() {
			final Long countO = (Long) item.getAttribute(ClusterFeatureAttribute.COUNT.attrName());
			return (countO != null) ? countO.longValue() : 0;
		}

		@Override
		public int getIterationID() {
			return ((Integer) item.getAttribute(ClusterFeatureAttribute.ITERATION.attrName())).intValue();
		}

		@Override
		public String getGroupID() {
			return getAttribute(
					item,
					ClusterFeatureAttribute.GROUP_ID.attrName());
		}

		@Override
		public void setGroupID(
				final String groupID ) {
			item.setAttribute(
					ClusterFeatureAttribute.GROUP_ID.attrName(),
					groupID);
		}

		@Override
		public void resetAssociatonCount() {
			item.setAttribute(
					ClusterFeatureAttribute.COUNT.attrName(),
					0);
		}

		@Override
		public void incrementAssociationCount(
				final long increment ) {
			item.setAttribute(
					ClusterFeatureAttribute.COUNT.attrName(),
					getAssociationCount() + increment);
		}

		@Override
		public String toString() {
			return "SimpleFeatureCentroid [item=" + item.getID() + ", + group=" + getGroupID() + ", + count="
					+ getAssociationCount() + ", cost=" + getCost() + "]";
		}

		@Override
		public double getCost() {
			final Double costO = (Double) item.getAttribute(ClusterFeatureAttribute.WEIGHT.attrName());
			return (costO != null) ? costO.doubleValue() : 0.0;
		}

		@Override
		public void setCost(
				final double cost ) {
			// GENERIC GEOMETRY HAS A DISTANCE, NOT A COST
			item.setAttribute(
					ClusterFeatureAttribute.WEIGHT.attrName(),
					cost);
		}

		@Override
		public String getName() {
			return item.getAttribute(
					ClusterFeatureAttribute.NAME.attrName()).toString();
		}

		@Override
		public String[] getExtraDimensions() {
			return new String[0];
		}

		@Override
		public double[] getDimensionValues() {
			return new double[0];
		}

		@Override
		public Geometry getGeometry() {
			return (Geometry) item.getAttribute(ClusterFeatureAttribute.GEOMETRY.attrName());
		}

		@Override
		public void setZoomLevel(
				final int level ) {
			item.setAttribute(
					ClusterFeatureAttribute.ZOOM_LEVEL.attrName(),
					Integer.valueOf(level));

		}

		@Override
		public int getZoomLevel() {
			return getIntAttribute(
					item,
					ClusterFeatureAttribute.ZOOM_LEVEL.attrName(),
					1);
		}

		@Override
		public void setBatchID(
				final String batchID ) {
			item.setAttribute(
					ClusterFeatureAttribute.BATCH_ID.attrName(),
					batchID);
		}

		@Override
		public String getBatchID() {
			return item.getAttribute(
					ClusterFeatureAttribute.BATCH_ID.attrName()).toString();
		}

	}

	private static String getAttribute(
			final SimpleFeature feature,
			final String name ) {
		final Object att = feature.getAttribute(name);
		return att == null ? null : att.toString();
	}

	private static int getIntAttribute(
			final SimpleFeature feature,
			final String name,
			final int defaultValue ) {
		final Object att = feature.getAttribute(name);
		return att == null ? defaultValue : (att instanceof Number ? ((Number) att).intValue() : Integer.parseInt(att
				.toString()));
	}

	/*
	 * @see
	 * mil.nga.giat.geowave.analytics.tools.CentroidFactory#createNextCentroid
	 * (java.lang.Object, com.vividsolutions.jts.geom.Coordinate,
	 * java.lang.String[], double[])
	 */

	@Override
	public AnalyticItemWrapper<SimpleFeature> createNextItem(
			final SimpleFeature feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		final Geometry geometry = (Geometry) feature.getAttribute(ClusterFeatureAttribute.GEOMETRY.attrName());

		return new SimpleFeatureAnalyticItemWrapper(
				AnalyticFeature.createGeometryFeature(
						feature.getFeatureType(),
						feature.getAttribute(
								ClusterFeatureAttribute.BATCH_ID.attrName()).toString(),
						UUID.randomUUID().toString(),
						getAttribute(
								feature,
								ClusterFeatureAttribute.NAME.attrName()),
						groupID,
						((Double) feature.getAttribute(ClusterFeatureAttribute.WEIGHT.attrName())).doubleValue(),
						geometry.getFactory().createPoint(
								coordinate),
						extraNames,
						extraValues,
						((Integer) feature.getAttribute(ClusterFeatureAttribute.ZOOM_LEVEL.attrName())).intValue(),
						((Integer) feature.getAttribute(ClusterFeatureAttribute.ITERATION.attrName())).intValue() + 1,
						((Long) feature.getAttribute(ClusterFeatureAttribute.COUNT.attrName())).longValue()));

	}

}
