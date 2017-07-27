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
package mil.nga.giat.geowave.analytic.clustering;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;

import com.vividsolutions.jts.geom.Geometry;

public class LongCentroid implements
		AnalyticItemWrapper<Long>
{

	Long val;
	long count = 0;
	double cost = 0.0;
	String groupID = "";

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + (int) (count ^ (count >>> 32));
		result = (prime * result) + ((val == null) ? 0 : val.hashCode());
		return result;
	}

	@Override
	public int getIterationID() {
		return 0;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final LongCentroid other = (LongCentroid) obj;
		if (count != other.count) {
			return false;
		}
		if (val == null) {
			if (other.val != null) {
				return false;
			}
		}
		else if (!val.equals(other.val)) {
			return false;
		}
		return true;
	}

	public LongCentroid(
			final long val,
			final String groupID,
			final int count ) {
		super();
		this.groupID = groupID;
		this.val = Long.valueOf(val);
		this.count = count;
	}

	@Override
	public String getGroupID() {
		return groupID;
	}

	@Override
	public String getID() {
		return val.toString();
	}

	@Override
	public Long getWrappedItem() {
		return val;
	}

	@Override
	public long getAssociationCount() {
		return count;
	}

	@Override
	public void resetAssociatonCount() {
		count = 0;
	}

	@Override
	public void incrementAssociationCount(
			final long increment ) {
		count++;
	}

	@Override
	public double getCost() {
		return cost;
	}

	@Override
	public void setCost(
			final double cost ) {
		this.cost = cost;

	}

	@Override
	public String toString() {
		return "LongCentroid [val=" + val + ", count=" + count + ", cost=" + cost + "]";
	}

	@Override
	public String getName() {
		return Long.toString(val);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setZoomLevel(
			final int level ) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getZoomLevel() {
		// TODO Auto-generated method stub
		return 1;
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
		this.groupID = groupID;

	}

}
