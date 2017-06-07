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
import java.util.NoSuchElementException;

/**
 * This interface wraps both the Iterator interface and the Closeable interface.
 * It is best to close an iterator of this interface when it is no longer
 * needed.
 * 
 * @param <E>
 *            A generic for the type of data for iteration
 */
public interface CloseableIterator<E> extends
		Iterator<E>,
		Closeable
{
	public static class Wrapper<E> implements
			CloseableIterator<E>
	{
		private final Iterator<E> iterator;

		public Wrapper(
				final Iterator<E> iterator ) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public E next() {
			return iterator.next();
		}

		@Override
		public void remove() {
			iterator.remove();
		}

		@Override
		public void close()
				throws IOException {
			// just a pass through on close()
		}
	}

	public static class Empty<E> implements
			CloseableIterator<E>
	{

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public E next()
				throws NoSuchElementException {
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {}

		@Override
		public void close()
				throws IOException {}
	}
}
