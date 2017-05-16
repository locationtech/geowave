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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytic.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

public class ClusterNeighborList implements
		NeighborList<ClusterItem>
{
	private final ByteArrayId id;
	final Map<ByteArrayId, Cluster> index;
	final NeighborListFactory<ClusterItem> factory;

	public ClusterNeighborList(
			final ByteArrayId centerId,
			final ClusterItem center,
			final NeighborListFactory<ClusterItem> factory,
			final Map<ByteArrayId, Cluster> index ) {
		super();
		this.index = index;
		this.id = centerId;
		this.factory = factory;
		Cluster cluster = getCluster();
		if (cluster == null) {
			cluster = (Cluster) factory.buildNeighborList(
					id,
					center);
			index.put(
					id,
					cluster);
		}
	}

	public Cluster getCluster() {
		return index.get(id);
	}

	@Override
	public Iterator<Entry<ByteArrayId, ClusterItem>> iterator() {
		return getCluster().iterator();
	}

	@Override
	public boolean add(
			DistanceProfile<?> distanceProfile,
			final ByteArrayId id,
			final ClusterItem value ) {
		Cluster cluster = index.get(id);
		if (cluster == null) {
			cluster = (Cluster) factory.buildNeighborList(
					id,
					value);
			index.put(
					id,
					cluster);
		}
		return getCluster().add(
				distanceProfile,
				id,
				value);
	}

	@Override
	public InferType infer(
			final ByteArrayId id,
			final ClusterItem value ) {
		return getCluster().infer(
				id,
				value);
	}

	@Override
	public void clear() {
		getCluster().clear();

	}

	@Override
	public int size() {
		return getCluster().size();
	}

	@Override
	public boolean isEmpty() {
		return getCluster().isEmpty();
	}

	public static class ClusterNeighborListFactory implements
			NeighborListFactory<ClusterItem>
	{
		final Map<ByteArrayId, Cluster> index;
		final NeighborListFactory<ClusterItem> factory;

		public ClusterNeighborListFactory(
				NeighborListFactory<ClusterItem> factory,
				Map<ByteArrayId, Cluster> index ) {
			super();
			this.index = index;
			this.factory = factory;
		}

		public Map<ByteArrayId, Cluster> getIndex() {
			return index;
		}

		@Override
		public NeighborList<ClusterItem> buildNeighborList(
				ByteArrayId centerId,
				ClusterItem center ) {
			return new ClusterNeighborList(
					centerId,
					center,
					factory,
					index);
		}
	}
}
