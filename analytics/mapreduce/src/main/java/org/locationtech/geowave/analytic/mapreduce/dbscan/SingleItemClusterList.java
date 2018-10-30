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
package org.locationtech.geowave.analytic.mapreduce.dbscan;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.analytic.mapreduce.dbscan.ClusterItemDistanceFn.ClusterProfileContext;
import org.locationtech.geowave.analytic.nn.DistanceProfile;
import org.locationtech.geowave.analytic.nn.NeighborList;
import org.locationtech.geowave.analytic.nn.NeighborListFactory;
import org.locationtech.geowave.core.index.ByteArray;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * Maintains a single hull around a set of points.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public class SingleItemClusterList extends
		DBScanClusterList implements
		Cluster
{

	private boolean compressed = false;
	private Set<Coordinate> clusterPoints = null;

	public SingleItemClusterList(
			final ByteArray centerId,
			final ClusterItem center,
			final NeighborListFactory<ClusterItem> factory,
			final Map<ByteArray, Cluster> index ) {
		super(
				center.getGeometry() instanceof Point || center.isCompressed() ? center.getGeometry() : null,
				(int) center.getCount(),
				centerId,
				index);

		final Geometry clusterGeo = center.getGeometry();

		compressed = center.isCompressed();

		if (compressed) {
			getClusterPoints(
					true).add(
					clusterGeo.getCentroid().getCoordinate());
		}
	}

	protected Set<Coordinate> getClusterPoints(
			boolean allowUpdates ) {
		if (clusterPoints == null || clusterPoints == Collections.<Coordinate> emptySet())
			clusterPoints = allowUpdates ? new HashSet<Coordinate>() : Collections.<Coordinate> emptySet();
		return clusterPoints;

	}

	@Override
	public void clear() {
		super.clear();
		clusterPoints = null;
	}

	@Override
	protected long addAndFetchCount(
			final ByteArray id,
			final ClusterItem newInstance,
			final DistanceProfile<?> distanceProfile ) {
		final ClusterProfileContext context = (ClusterProfileContext) distanceProfile.getContext();

		boolean checkForCompress = false;

		final Coordinate centerCoordinate = context.getItem1() == newInstance ? context.getPoint2() : context
				.getPoint1();

		Geometry thisGeo = getGeometry();
		// only need to cluster this new point if it is likely top be an
		// inter-segment point
		if (thisGeo == null || !(thisGeo instanceof Point)) {
			checkForCompress = getClusterPoints(
					true).add(
					centerCoordinate);
		}

		// Closest distance points are only added if they are on a segment of a
		// complex geometry.
		if (!(newInstance.getGeometry() instanceof Point)) {
			final Coordinate newInstanceCoordinate = context.getItem2() == newInstance ? context.getPoint2() : context
					.getPoint1();
			checkForCompress = getClusterPoints(
					true).add(
					newInstanceCoordinate);
		}

		if (checkForCompress) checkForCompression();
		return 1;
	}

	@Override
	public void merge(
			Cluster cluster ) {
		if (this == cluster) return;

		final SingleItemClusterList singleItemCluster = ((SingleItemClusterList) cluster);

		super.merge(cluster);

		if (singleItemCluster.clusterGeo != null) {
			getClusterPoints(
					true).addAll(
					Arrays.asList(singleItemCluster.clusterGeo.getCoordinates()));
		}

		Set<Coordinate> otherPoints = singleItemCluster.getClusterPoints(false);
		if (otherPoints.size() > 0) {
			// handle any remaining points
			getClusterPoints(
					true).addAll(
					otherPoints);
		}

		checkForCompression();
	}

	public boolean isCompressed() {
		return compressed;
	}

	public void finish() {
		super.finish();
		compressAndUpdate();
	}

	private void checkForCompression() {
		if (getClusterPoints(
				false).size() > 50) {
			compressAndUpdate();
		}
	}

	private void compressAndUpdate() {
		clusterGeo = compress();
		clusterPoints = null;
		compressed = true;
	}

	protected Geometry compress() {
		if (getClusterPoints(
				false).size() > 0) {
			return DBScanClusterList.getHullTool().createHullFromGeometry(
					clusterGeo,
					clusterPoints,
					true);
		}
		return clusterGeo;

	}

	public static class SingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArray, Cluster> index;

		public SingleItemClusterListFactory(
				final Map<ByteArray, Cluster> index ) {
			super();
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArray centerId,
				final ClusterItem center ) {
			Cluster list = index.get(centerId);
			if (list == null) {
				list = new SingleItemClusterList(
						centerId,
						center,
						this,
						index);

			}
			return list;
		}
	}
}
