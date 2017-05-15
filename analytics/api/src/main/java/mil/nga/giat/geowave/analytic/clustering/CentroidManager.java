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

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Manage centroids created per batch and per group of analytic processes. There
 * can be multiple groups per batch. A group is loosely interpreted as a set of
 * item geometries under analysis. The sets can be defined by shared
 * characteristics.
 * 
 * @param <T>
 *            The type of item that is used to represent a centroid.
 */
public interface CentroidManager<T>
{

	/**
	 * Creates a new centroid based on the old centroid with new coordinates and
	 * dimension values
	 * 
	 * @param feature
	 * @param coordinate
	 * @param extraNames
	 * @param extraValues
	 * @return
	 */
	public AnalyticItemWrapper<T> createNextCentroid(
			final T feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues );

	public AnalyticItemWrapper<T> getCentroidById(
			final String id,
			final String groupID )
			throws IOException,
			MatchingCentroidNotFoundException;

	public void delete(
			final String[] dataIds )
			throws IOException;

	public List<String> getAllCentroidGroups()
			throws IOException;

	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String groupID )
			throws IOException;

	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String batchID,
			final String groupID )
			throws IOException;

	public int processForAllGroups(
			CentroidProcessingFn<T> fn )
			throws IOException;

	public static interface CentroidProcessingFn<T>
	{
		public int processGroup(
				final String groupID,
				final List<AnalyticItemWrapper<T>> centroids );
	}

	public AnalyticItemWrapper<T> getCentroid(
			final String id );

	public void clear();

	public ByteArrayId getDataTypeId();

	public ByteArrayId getIndexId();
}
