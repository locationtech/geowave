/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.dbscan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.locationtech.geowave.analytic.GeometryHullTool;
import org.locationtech.geowave.analytic.nn.DistanceProfile;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.TopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a cluster. Maintains links to other clusters through shared components Maintains
 * counts contributed by components of this cluster. Supports merging with other clusters,
 * incrementing the count by only those components different from the other cluster.
 *
 * <p> Intended to run in a single thread. Not Thread Safe.
 */
public abstract class DBScanClusterList implements Cluster {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanClusterList.class);

  // internal state
  protected Geometry clusterGeo = null;
  protected int itemCount = 1;
  private Set<ByteArray> linkedClusters = null;
  private List<ByteArray> ids = null;
  private final ByteArray id;

  // global configuration...to save memory...passing this stuff around.
  private static GeometryHullTool connectGeometryTool = new GeometryHullTool();
  private static int mergeSize = 0;

  // global state
  // ID to cluster.
  protected final Map<ByteArray, Cluster> index;

  public static GeometryHullTool getHullTool() {
    return connectGeometryTool;
  }

  public static void setMergeSize(final int size) {
    mergeSize = size;
  }

  public DBScanClusterList(
      final Geometry clusterGeo,
      final int itemCount,
      final ByteArray centerId,
      final Map<ByteArray, Cluster> index) {
    super();
    this.clusterGeo = clusterGeo;
    this.itemCount = itemCount;
    this.index = index;
    id = centerId;
  }

  protected abstract long addAndFetchCount(
      final ByteArray newId,
      final ClusterItem newInstance,
      final DistanceProfile<?> distanceProfile);

  @Override
  public final boolean add(
      final DistanceProfile<?> distanceProfile,
      final ByteArray newId,
      final ClusterItem newInstance) {

    LOGGER.trace("link {} to {}", newId, id);

    if (!getLinkedClusters(true).add(newId)) {
      return false;
    }

    final Cluster cluster = index.get(newId);

    if (cluster == this) {
      return false;
    }

    incrementItemCount(addAndFetchCount(newId, newInstance, distanceProfile));

    return true;
  }

  protected List<ByteArray> getIds(final boolean allowUpdates) {
    if ((ids == null) || (ids == Collections.<ByteArray>emptyList())) {
      ids = allowUpdates ? new ArrayList<>(4) : Collections.<ByteArray>emptyList();
    }
    return ids;
  }

  protected Set<ByteArray> getLinkedClusters(final boolean allowUpdates) {
    if ((linkedClusters == null) || (linkedClusters == Collections.<ByteArray>emptySet())) {
      linkedClusters = allowUpdates ? new HashSet<>() : Collections.<ByteArray>emptySet();
    }
    return linkedClusters;
  }

  protected void incrementItemCount(final long amount) {
    final int c = itemCount;
    itemCount += amount;
    assert (c <= itemCount);
  }

  /**
   * Clear the contents. Invoked when the contents of a cluster are merged with another cluster.
   * This method is supportive for GC, not serving any algorithm logic.
   */
  @Override
  public void clear() {
    linkedClusters = null;
    clusterGeo = null;
  }

  @Override
  public void invalidate() {
    for (final ByteArray linkedId : getLinkedClusters(true)) {
      final Cluster linkedCluster = index.get(linkedId);
      if ((linkedCluster != null)
          && (linkedCluster != this)
          && (linkedCluster instanceof DBScanClusterList)) {
        ((DBScanClusterList) linkedCluster).getLinkedClusters(false).remove(id);
      }
    }
    LOGGER.trace("Invalidate " + id);
    index.remove(id);
    linkedClusters = null;
    clusterGeo = null;
    itemCount = -1;
  }

  @Override
  public InferType infer(final ByteArray id, final ClusterItem value) {
    final Cluster cluster = index.get(id);
    if ((cluster == this) || getLinkedClusters(false).contains(id)) {
      return InferType.SKIP;
    }
    return InferType.NONE;
  }

  @Override
  public Iterator<Entry<ByteArray, ClusterItem>> iterator() {
    return Collections.<Entry<ByteArray, ClusterItem>>emptyList().iterator();
  }

  @Override
  public int currentLinkSetSize() {
    return getLinkedClusters(false).size();
  }

  @Override
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
  public boolean equals(final Object obj) {
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
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public int size() {
    return (itemCount);
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
  public void merge(final Cluster cluster) {
    final boolean removedLinked = getLinkedClusters(true).remove(cluster.getId());
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Merging {} into {}", cluster.getId(), id);
    }
    if (cluster != this) {
      getIds(true).add(cluster.getId());
      index.put(cluster.getId(), this);

      if (cluster instanceof DBScanClusterList) {
        for (final ByteArray id : ((DBScanClusterList) cluster).getIds(false)) {
          index.put(id, this);
          ids.add(id);
        }
        getLinkedClusters(true).addAll(((DBScanClusterList) cluster).getLinkedClusters(false));
      }

      if (isCompressed() && ((DBScanClusterList) cluster).isCompressed()) {
        incrementItemCount(
            (long) (interpolateFactor(((DBScanClusterList) cluster).clusterGeo)
                * ((DBScanClusterList) cluster).itemCount));
      } else if (!removedLinked) {
        incrementItemCount(1);
      }
    }
  }

  protected double interpolateFactor(final Geometry areaBeingMerged) {
    try {
      if (clusterGeo == null) {
        return 1.0;
      }
      final Geometry intersection = areaBeingMerged.intersection(clusterGeo);
      final double geo2Area = areaBeingMerged.getArea();
      if (intersection != null) {
        if ((intersection instanceof Point) && (areaBeingMerged instanceof Point)) {
          return 0.0;
        } else if (intersection.isEmpty()) {
          return 1.0;
        } else if (geo2Area > 0) {
          return 1.0 - (intersection.getArea() / geo2Area);
        } else {
          return 0.0;
        }
      }
      return 1.0;
    } catch (final Exception ex) {
      LOGGER.warn("Cannot calculate difference of geometries to interpolate size ", ex);
    }
    return 0.0;
  }

  @Override
  public ByteArray getId() {
    return id;
  }

  protected abstract Geometry compress();

  @Override
  public Set<ByteArray> getLinkedClusters() {
    return getLinkedClusters(false);
  }

  protected void union(final Geometry otherGeo) {

    if (otherGeo == null) {
      return;
    }
    try {

      if (clusterGeo == null) {
        clusterGeo = otherGeo;
      } else if (clusterGeo instanceof Point) {
        clusterGeo = connectGeometryTool.connect(otherGeo, clusterGeo);
      } else {
        clusterGeo = connectGeometryTool.connect(clusterGeo, otherGeo);
      }
    } catch (final TopologyException ex) {

      LOGGER.error("Union failed due to non-simple geometries", ex);
      clusterGeo =
          connectGeometryTool.createHullFromGeometry(
              clusterGeo,
              Arrays.asList(otherGeo.getCoordinates()),
              false);
    }
  }

  protected void mergeLinks(final boolean deleteNonLinks) {
    if (getLinkedClusters(false).size() == 0) {
      return;
    }

    final Set<Cluster> readyClusters = new HashSet<>();

    readyClusters.add(this);
    buildClusterLists(readyClusters, this, deleteNonLinks);

    readyClusters.remove(this);
    final Iterator<Cluster> finishedIt = readyClusters.iterator();
    final Cluster top = this;
    while (finishedIt.hasNext()) {
      top.merge(finishedIt.next());
    }
  }

  private void buildClusterLists(
      final Set<Cluster> readyClusters,
      final DBScanClusterList cluster,
      final boolean deleteNonLinks) {
    for (final ByteArray linkedClusterId : cluster.getLinkedClusters()) {
      final Cluster linkedCluster = index.get(linkedClusterId);
      if (readyClusters.add(linkedCluster) && (linkedCluster.size() >= mergeSize)) {
        buildClusterLists(readyClusters, (DBScanClusterList) linkedCluster, false);
      }
    }
  }

  @Override
  public String toString() {
    return "DBScanClusterList [clusterGeo="
        + (clusterGeo == null ? "null" : clusterGeo.toString())
        + ", id="
        + id
        + "]";
  }
}
