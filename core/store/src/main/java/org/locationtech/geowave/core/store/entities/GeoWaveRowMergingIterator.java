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
package org.locationtech.geowave.core.store.entities;

import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class GeoWaveRowMergingIterator<T extends MergeableGeoWaveRow> implements
		Iterator<T>
{

	final Iterator<T> source;
	final PeekingIterator<T> peekingIterator;

	public GeoWaveRowMergingIterator(
			final Iterator<T> source ) {
		this.source = source;
		this.peekingIterator = Iterators.peekingIterator(source);
	}

	@Override
	public boolean hasNext() {
		return peekingIterator.hasNext();
	}

	@Override
	public T next() {
		final T nextValue = peekingIterator.next();
		while (peekingIterator.hasNext() && nextValue.shouldMerge(peekingIterator.peek())) {
			nextValue.mergeRow(peekingIterator.next());
		}
		return nextValue;
	}
}
