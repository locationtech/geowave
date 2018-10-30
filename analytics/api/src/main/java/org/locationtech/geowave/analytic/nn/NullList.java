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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.locationtech.geowave.core.index.ByteArray;

public class NullList<NNTYPE> implements
		NeighborList<NNTYPE>
{

	@Override
	public boolean add(
			final DistanceProfile<?> distanceProfile,
			final ByteArray id,
			final NNTYPE value ) {
		return false;
	}

	@Override
	public InferType infer(
			final ByteArray id,
			final NNTYPE value ) {
		return InferType.SKIP;
	}

	@Override
	public void clear() {

	}

	@Override
	public Iterator<Entry<ByteArray, NNTYPE>> iterator() {
		return Collections.emptyIterator();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

}
