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

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * Wrap an object used to by analytical processes. This class provides generic
 * wrapper to specific functions associated with analytic processes such as
 * managing centroids.
 * 
 * 
 * @param <T>
 */
public interface AnalyticItemWrapper<T>
{
	public String getID();

	public T getWrappedItem();

	public long getAssociationCount();

	public void resetAssociatonCount();

	public void incrementAssociationCount(
			long increment );

	public int getIterationID();

	public String getName();

	public String[] getExtraDimensions();

	public double[] getDimensionValues();

	public Geometry getGeometry();

	public double getCost();

	public void setCost(
			double cost );

	public String getGroupID();

	public void setGroupID(
			String groupID );

	public void setZoomLevel(
			int level );

	public int getZoomLevel();

	public void setBatchID(
			String batchID );

	public String getBatchID();

}
