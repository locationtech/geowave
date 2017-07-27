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

import java.util.Arrays;
import java.util.Map;

import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * 
 * Maintains a single hull around a set of points.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 */
public class PreProcessSingleItemClusterList extends
		SingleItemClusterList implements
		Cluster
{

	public PreProcessSingleItemClusterList(
			ByteArrayId centerId,
			ClusterItem center,
			NeighborListFactory<ClusterItem> factory,
			Map<ByteArrayId, Cluster> index ) {
		super(
				centerId,
				center,
				factory,
				index);
	}

	@Override
	protected void mergeLinks(
			final boolean deleteNonLinks ) {
		for (ByteArrayId id : this.getLinkedClusters()) {
			PreProcessSingleItemClusterList other = (PreProcessSingleItemClusterList) index.get(id);
			final long snapShot = getClusterPoints(
					false).size();
			if (other.clusterGeo != null) {
				getClusterPoints(
						true).addAll(
						Arrays.asList(other.clusterGeo.getCoordinates()));
			}
			getClusterPoints(
					true).addAll(
					other.getClusterPoints(false));
			incrementItemCount(getClusterPoints(
					true).size() - snapShot);
		}

	}

	public static class PreProcessSingleItemClusterListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster> index;

		public PreProcessSingleItemClusterListFactory(
				final Map<ByteArrayId, Cluster> index ) {
			super();
			this.index = index;
		}

		@Override
		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			Cluster list = index.get(centerId);
			if (list == null) {
				list = new PreProcessSingleItemClusterList(
						centerId,
						center,
						this,
						index);

			}
			return list;
		}
	}
}
