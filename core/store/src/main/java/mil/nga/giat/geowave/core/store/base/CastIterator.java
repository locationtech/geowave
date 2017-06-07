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
package mil.nga.giat.geowave.core.store.base;

import java.util.Iterator;

import mil.nga.giat.geowave.core.store.CloseableIterator;

public class CastIterator<T> implements
		Iterator<CloseableIterator<T>>
{

	final Iterator<CloseableIterator<Object>> it;

	public CastIterator(
			final Iterator<CloseableIterator<Object>> it ) {
		this.it = it;
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public CloseableIterator<T> next() {
		return (CloseableIterator<T>) it.next();
	}

	@Override
	public void remove() {
		it.remove();
	}
}
