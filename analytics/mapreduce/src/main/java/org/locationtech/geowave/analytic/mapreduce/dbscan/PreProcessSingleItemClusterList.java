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
import java.util.Map;

import org.locationtech.geowave.analytic.nn.NeighborList;
import org.locationtech.geowave.analytic.nn.NeighborListFactory;
import org.locationtech.geowave.core.index.ByteArray;

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
			ByteArray centerId,
			ClusterItem center,
			NeighborListFactory<ClusterItem> factory,
			Map<ByteArray, Cluster> index ) {
		super(
				centerId,
				center,
				factory,
				index);
	}

	@Override
	protected void mergeLinks(
			final boolean deleteNonLinks ) {
		for (ByteArray id : this.getLinkedClusters()) {
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
		private final Map<ByteArray, Cluster> index;

		public PreProcessSingleItemClusterListFactory(
				final Map<ByteArray, Cluster> index ) {
			super();
			this.index = index;
		}

		@Override
		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArray centerId,
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
