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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class TestObjectItemWrapperFactory implements
		AnalyticItemWrapperFactory<TestObject>
{

	@Override
	public AnalyticItemWrapper<TestObject> create(
			final TestObject item ) {
		return new TestObjectItemWrapper(
				item);
	}

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws IOException {}

	@Override
	public AnalyticItemWrapper<TestObject> createNextItem(
			final TestObject feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		final TestObject obj = new TestObject();
		obj.groupID = groupID;
		obj.geo = feature.geo.getFactory().createPoint(
				coordinate);
		obj.name = feature.name;
		return new TestObjectItemWrapper(
				obj);
	}

	static class TestObjectItemWrapper implements
			AnalyticItemWrapper<TestObject>
	{

		private final TestObject item;

		public TestObjectItemWrapper(
				final TestObject item ) {
			super();
			this.item = item;
		}

		@Override
		public String getID() {
			return item.id;
		}

		@Override
		public String getGroupID() {
			return item.groupID;
		}

		@Override
		public TestObject getWrappedItem() {
			return item;
		}

		@Override
		public long getAssociationCount() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void resetAssociatonCount() {
			// TODO Auto-generated method stub

		}

		@Override
		public void incrementAssociationCount(
				final long increment ) {
			// TODO Auto-generated method stub

		}

		@Override
		public int getIterationID() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getName() {
			return item.id;
		}

		@Override
		public String[] getExtraDimensions() {
			return new String[] {};
		}

		@Override
		public double[] getDimensionValues() {
			return new double[0];
		}

		@Override
		public Geometry getGeometry() {
			return item.geo;
		}

		@Override
		public double getCost() {
			return 0;
		}

		@Override
		public void setCost(
				final double cost ) {
			// TODO Auto-generated method stub

		}

		@Override
		public void setZoomLevel(
				final int level ) {
			item.setLevel(level);

		}

		@Override
		public int getZoomLevel() {
			return item.getLevel();
		}

		@Override
		public void setBatchID(
				final String batchID ) {
			// TODO Auto-generated method stub

		}

		@Override
		public String getBatchID() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setGroupID(
				final String groupID ) {
			item.groupID = groupID;

		}

	}

}
