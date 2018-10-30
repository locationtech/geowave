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
package org.locationtech.geowave.analytic.nn;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.locationtech.geowave.core.index.ByteArray;

public class DefaultNeighborList<NNTYPE> implements
		NeighborList<NNTYPE>
{
	private final Map<ByteArray, NNTYPE> list = new HashMap<ByteArray, NNTYPE>();

	@Override
	public boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArray id,
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
			final ByteArray id,
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
	public Iterator<Entry<ByteArray, NNTYPE>> iterator() {
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
				final ByteArray centerId,
				final NNTYPE center ) {
			return new DefaultNeighborList<NNTYPE>();
		}
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	public NNTYPE get(
			final ByteArray key ) {
		return list.get(key);
	}

}
