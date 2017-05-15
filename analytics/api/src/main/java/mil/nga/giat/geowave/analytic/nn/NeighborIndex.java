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
package mil.nga.giat.geowave.analytic.nn;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * Maintain an association between an ID of any item and its neighbors, as they
 * are discovered. The index supports a bi-directional association, forming a
 * graph of adjacency lists.
 * 
 * 
 * @param <NNTYPE>
 */
public class NeighborIndex<NNTYPE>
{
	private final Map<ByteArrayId, NeighborList<NNTYPE>> index = new HashMap<ByteArrayId, NeighborList<NNTYPE>>();
	private final NeighborListFactory<NNTYPE> listFactory;

	private final NullList<NNTYPE> nullList = new NullList<NNTYPE>();

	public NeighborIndex(
			final NeighborListFactory<NNTYPE> listFactory ) {
		super();
		this.listFactory = listFactory;
	}

	/**
	 * Invoked when the provided node is being inspected to find neighbors.
	 * Creates the associated neighbor list, if not already created. Notifies
	 * the neighbor list that it is formally initialized. The neighbor list may
	 * already exist and have associated neighbors. This occurs when those
	 * relationships are discovered through traversing the neighbor.
	 * 
	 * This method is designed for neighbor lists do some optimizations just
	 * prior to the neighbor discovery process.
	 * 
	 * @param node
	 * @return
	 */
	public NeighborList<NNTYPE> init(
			ByteArrayId id,
			NNTYPE value ) {
		NeighborList<NNTYPE> neighbors = index.get(id);
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(
					id,
					value);
			index.put(
					id,
					neighbors);
		}
		return neighbors;
	}

	public void add(
			final DistanceProfile<?> distanceProfile,
			ByteArrayId centerId,
			NNTYPE centerValue,
			ByteArrayId neighborId,
			NNTYPE neighborValue,
			final boolean addReciprical ) {
		this.addToList(
				distanceProfile,
				centerId,
				centerValue,
				neighborId,
				neighborValue);
		if (addReciprical) {
			this.addToList(
					distanceProfile,
					neighborId,
					neighborValue,
					centerId,
					centerValue);
		}
	}

	public void empty(
			final ByteArrayId id ) {
		index.put(
				id,
				nullList);
	}

	private void addToList(
			final DistanceProfile<?> distanceProfile,
			ByteArrayId centerId,
			NNTYPE centerValue,
			ByteArrayId neighborId,
			NNTYPE neighborValue ) {
		NeighborList<NNTYPE> neighbors = index.get(centerId);
		if (neighbors == null) {
			neighbors = listFactory.buildNeighborList(
					centerId,
					centerValue);
			index.put(
					centerId,
					neighbors);
		}
		neighbors.add(
				distanceProfile,
				neighborId,
				neighborValue);
	}

}
