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
package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple wrapper around an iterator and a closeable to ensure that
 * the caller can close the closeable when it is finished being used by the
 * iterator.
 * 
 * @param <E>
 *            The type to iterate on
 */
public class CloseableIteratorWrapper<E> implements
		CloseableIterator<E>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CloseableIteratorWrapper.class);

	private final Closeable closeable;
	private final Iterator<E> iterator;
	private Integer limit = null;
	private int count = 0;

	public CloseableIteratorWrapper(
			final Closeable closable,
			final Iterator<E> iterator ) {
		this.closeable = closable;
		this.iterator = iterator;
	}

	public CloseableIteratorWrapper(
			final Closeable closable,
			final Iterator<E> iterator,
			Integer limit ) {
		this.closeable = closable;
		this.iterator = iterator;
		this.limit = limit;
	}

	@Override
	public boolean hasNext() {
		final boolean hasNext = iterator.hasNext();
		if (!hasNext) {
			try {
				closeable.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close iterator",
						e);
			}
		}
		if (limit != null && limit > 0 && count > limit) return false;
		return hasNext;
	}

	@Override
	public E next() {
		count++;
		return iterator.next();
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	@Override
	protected void finalize()
			throws Throwable {
		super.finalize();
		closeable.close();
	}

	@Override
	public void close()
			throws IOException {
		try {
			closeable.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close iterator",
					e);
		}
	}
}
