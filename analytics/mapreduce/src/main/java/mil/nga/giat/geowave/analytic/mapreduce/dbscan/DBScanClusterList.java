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
package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.TopologyException;

/**
 * 
 * Represents a cluster. Maintains links to other clusters through shared
 * components Maintains counts contributed by components of this cluster.
 * Supports merging with other clusters, incrementing the count by only those
 * components different from the other cluster.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public abstract class DBScanClusterList implements
		Cluster
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanClusterList.class);

	// internal state
	protected Geometry clusterGeo = null;
	protected int itemCount = 1;
	private Set<ByteArrayId> linkedClusters = null;
	private List<ByteArrayId> ids = null;
	private ByteArrayId id;

	// global configuration...to save memory...passing this stuff around.
	private static GeometryHullTool connectGeometryTool = new GeometryHullTool();
	private static int mergeSize = 0;

	// global state
	// ID to cluster.
	protected final Map<ByteArrayId, Cluster> index;

	public static GeometryHullTool getHullTool() {
		return connectGeometryTool;
	}

	public static void setMergeSize(
			int size ) {
		mergeSize = size;
	}

	public DBScanClusterList(
			final Geometry clusterGeo,
			final int itemCount,
			final ByteArrayId centerId,
			final Map<ByteArrayId, Cluster> index ) {
		super();
		this.clusterGeo = clusterGeo;
		this.itemCount = itemCount;
		this.index = index;
		id = centerId;
	}

	protected abstract long addAndFetchCount(
			final ByteArrayId newId,
			final ClusterItem newInstance,
			final DistanceProfile<?> distanceProfile );

	@Override
	public final boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArrayId newId,
			final ClusterItem newInstance ) {

		LOGGER.trace(
				"link {} to {}",
				newId,
				id);

		if (!getLinkedClusters(
				true).add(
				newId)) return false;

		Cluster cluster = index.get(newId);

		if (cluster == this) return false;

		incrementItemCount(addAndFetchCount(
				newId,
				newInstance,
				distanceProfile));

		return true;
	}

	protected List<ByteArrayId> getIds(
			boolean allowUpdates ) {
		if (ids == null || ids == Collections.<ByteArrayId> emptyList())
			ids = allowUpdates ? new ArrayList<ByteArrayId>(
					4) : Collections.<ByteArrayId> emptyList();
		return ids;
	}

	protected Set<ByteArrayId> getLinkedClusters(
			boolean allowUpdates ) {
		if (linkedClusters == null || linkedClusters == Collections.<ByteArrayId> emptySet())
			linkedClusters = allowUpdates ? new HashSet<ByteArrayId>() : Collections.<ByteArrayId> emptySet();
		return linkedClusters;
	}

	protected void incrementItemCount(
			long amount ) {
		int c = itemCount;
		itemCount += amount;
		assert (c <= itemCount);
	}

	/**
	 * Clear the contents. Invoked when the contents of a cluster are merged
	 * with another cluster. This method is supportive for GC, not serving any
	 * algorithm logic.
	 */

	@Override
	public void clear() {
		linkedClusters = null;
		clusterGeo = null;
	}

	@Override
	public void invalidate() {
		for (ByteArrayId linkedId : getLinkedClusters(true)) {
			Cluster linkedCluster = index.get(linkedId);
			if (linkedCluster != null && linkedCluster != this && linkedCluster instanceof DBScanClusterList) {
				((DBScanClusterList) linkedCluster).getLinkedClusters(
						false).remove(
						id);
			}
		}
		LOGGER.trace("Invalidate " + id);
		index.remove(id);
		linkedClusters = null;
		clusterGeo = null;
		itemCount = -1;
	}

	@Override
	public InferType infer(
			final ByteArrayId id,
			final ClusterItem value ) {
		final Cluster cluster = index.get(id);
		if (cluster == this || getLinkedClusters(
				false).contains(
				id)) return InferType.SKIP;
		return InferType.NONE;
	}

	@Override
	public Iterator<Entry<ByteArrayId, ClusterItem>> iterator() {
		return Collections.<Entry<ByteArrayId, ClusterItem>> emptyList().iterator();
	}

	@Override
	public int currentLinkSetSize() {
		return getLinkedClusters(
				false).size();
	}

	public void finish() {
		mergeLinks(true);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((id == null) ? 0 : id.hashCode());
		return result;
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
		final DBScanClusterList other = (DBScanClusterList) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		}
		else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}

	@Override
	public int size() {
		return (int) (itemCount);
	}

	@Override
	public boolean isEmpty() {
		return size() <= 0;
	}

	@Override
	public Geometry getGeometry() {
		return compress();
	}

	@Override
	public abstract boolean isCompressed();

	@Override
	public void merge(
			final Cluster cluster ) {
		boolean removedLinked = getLinkedClusters(
				true).remove(
				cluster.getId());
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(
					"Merging {} into {}",
					cluster.getId(),
					this.id);
		}
		if (cluster != this) {
			getIds(
					true).add(
					cluster.getId());
			index.put(
					cluster.getId(),
					this);

			if (cluster instanceof DBScanClusterList) {
				for (ByteArrayId id : ((DBScanClusterList) cluster).getIds(false)) {
					index.put(
							id,
							this);
					this.ids.add(id);
				}
				getLinkedClusters(
						true).addAll(
						((DBScanClusterList) cluster).getLinkedClusters(false));
			}

			if (isCompressed() && ((DBScanClusterList) cluster).isCompressed()) {
				incrementItemCount((long) (interpolateFactor(((DBScanClusterList) cluster).clusterGeo) * ((DBScanClusterList) cluster).itemCount));
			}
			else if (!removedLinked) {
				incrementItemCount(1);
			}

		}
	}

	protected double interpolateFactor(
			final Geometry areaBeingMerged ) {
		try {
			if (clusterGeo == null) return 1.0;
			Geometry intersection = areaBeingMerged.intersection(clusterGeo);
			double geo2Area = areaBeingMerged.getArea();
			if (intersection != null) {
				if (intersection instanceof Point && areaBeingMerged instanceof Point)
					return 0.0;
				else if (intersection.isEmpty())
					return 1.0;
				else if (geo2Area > 0)
					return 1.0 - (intersection.getArea() / geo2Area);
				else
					return 0.0;
			}
			return 1.0;
		}
		catch (final Exception ex) {
			LOGGER.warn(
					"Cannot calculate difference of geometries to interpolate size ",
					ex);
		}
		return 0.0;

	}

	@Override
	public ByteArrayId getId() {
		return id;
	}

	protected abstract Geometry compress();

	@Override
	public Set<ByteArrayId> getLinkedClusters() {
		return getLinkedClusters(false);
	}

	protected void union(
			Geometry otherGeo ) {

		if (otherGeo == null) return;
		try {

			if (clusterGeo == null) {
				clusterGeo = otherGeo;
			}
			else if (clusterGeo instanceof Point) {
				clusterGeo = connectGeometryTool.connect(
						otherGeo,
						clusterGeo);
			}
			else {
				clusterGeo = connectGeometryTool.connect(
						clusterGeo,
						otherGeo);
			}
		}
		catch (TopologyException ex) {

			LOGGER.error(
					"Union failed due to non-simple geometries",
					ex);
			clusterGeo = connectGeometryTool.createHullFromGeometry(
					clusterGeo,
					Arrays.asList(otherGeo.getCoordinates()),
					false);
		}
	}

	protected void mergeLinks(
			final boolean deleteNonLinks ) {
		if (getLinkedClusters(
				false).size() == 0) return;

		final Set<Cluster> readyClusters = new HashSet<Cluster>();

		readyClusters.add(this);
		buildClusterLists(
				readyClusters,
				this,
				deleteNonLinks);

		readyClusters.remove(this);
		final Iterator<Cluster> finishedIt = readyClusters.iterator();
		Cluster top = this;
		while (finishedIt.hasNext()) {
			top.merge(finishedIt.next());
		}
	}

	private void buildClusterLists(
			final Set<Cluster> readyClusters,
			final DBScanClusterList cluster,
			final boolean deleteNonLinks ) {
		for (final ByteArrayId linkedClusterId : cluster.getLinkedClusters()) {
			final Cluster linkedCluster = index.get(linkedClusterId);
			if (readyClusters.add(linkedCluster) && linkedCluster.size() >= mergeSize) {
				buildClusterLists(
						readyClusters,
						(DBScanClusterList) linkedCluster,
						false);
			}
		}
	}

	@Override
	public String toString() {
		return "DBScanClusterList [clusterGeo=" + (clusterGeo == null ? "null" : clusterGeo.toString()) + ", id=" + id
				+ "]";
	}

}
