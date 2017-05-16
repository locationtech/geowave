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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class DefaultNeighborList<NNTYPE> implements
		NeighborList<NNTYPE>
{
	private final Map<ByteArrayId, NNTYPE> list = new HashMap<ByteArrayId, NNTYPE>();

	@Override
	public boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArrayId id,
			final NNTYPE value ) {
		if (infer(
				id,
				value) == InferType.NONE) {
			list.put(
					id,
					value);
			return true;
		}
		return false;

	}

	@Override
	public InferType infer(
			final ByteArrayId id,
			final NNTYPE value ) {
		if (list.containsKey(id)) {
			return InferType.SKIP;
		}
		return InferType.NONE;
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public Iterator<Entry<ByteArrayId, NNTYPE>> iterator() {
		return list.entrySet().iterator();
	}

	@Override
	public int size() {
		return list.size();
	}

	public static class DefaultNeighborListFactory<NNTYPE> implements
			NeighborListFactory<NNTYPE>
	{
		@Override
		public NeighborList<NNTYPE> buildNeighborList(
				final ByteArrayId centerId,
				final NNTYPE center ) {
			return new DefaultNeighborList<NNTYPE>();
		}
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	public NNTYPE get(
			final ByteArrayId key ) {
		return list.get(key);
	}

}
