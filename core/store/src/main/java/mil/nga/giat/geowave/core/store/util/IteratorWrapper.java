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
package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * This class is used internally within the ingest process of GeoWave to convert
 * each entry into a set of mutations and iterate through them (maintaining a
 * queue of mutations internally in the case where a single entry converts to
 * multiple mutations). It is generalized to wrap any iterator with a converter
 * to a list of a different type.
 * 
 * @param <InputType>
 *            The type of the input iterator
 * @param <ConvertedType>
 *            The type of the new converted iterator
 */
public class IteratorWrapper<InputType, ConvertedType> implements
		Iterator<ConvertedType>
{
	public static interface Converter<InputType, ConvertedType>
	{
		public Iterator<ConvertedType> convert(
				InputType entry );
	}

	public static interface Callback<InputType, ConvertedType>
	{
		public void notifyIterationComplete(
				InputType entry );
	}

	final private Iterator<InputType> inputIterator;
	final private Converter<InputType, ConvertedType> converter;
	private Iterator<ConvertedType> conversionQueue = new LinkedList<ConvertedType>().iterator();
	private final Callback<InputType, ConvertedType> conversionCallback;
	private InputType lastInput;

	public IteratorWrapper(
			final Iterator<InputType> inputIterator,
			final Converter<InputType, ConvertedType> converter ) {
		this(
				inputIterator,
				converter,
				null);
	}

	public IteratorWrapper(
			final Iterator<InputType> inputIterator,
			final Converter<InputType, ConvertedType> converter,
			final Callback<InputType, ConvertedType> conversionCallback ) {
		this.inputIterator = inputIterator;
		this.converter = converter;
		this.conversionCallback = conversionCallback;
	}

	@Override
	public synchronized boolean hasNext() {
		if (conversionQueue.hasNext()) {
			return true;
		}
		return inputIterator.hasNext();
	}

	@Override
	public synchronized ConvertedType next() {
		while (!conversionQueue.hasNext() && inputIterator.hasNext()) {
			// fill conversion queue with converted objects from the next input
			final InputType input = inputIterator.next();
			final Iterator<ConvertedType> conversions = converter.convert(input);

			lastInput = input;
			conversionQueue = conversions;
		}
		final ConvertedType retVal = conversionQueue.next();
		if (!conversionQueue.hasNext() && (conversionCallback != null)) {
			// if the queue is empty, then notify that the last input had been
			// converted and iterated on
			notifyIterationComplete();
		}
		return retVal;
	}

	private synchronized void notifyIterationComplete() {
		if (lastInput != null) {
			if (conversionCallback != null) {
				conversionCallback.notifyIterationComplete(lastInput);
			}
			lastInput = null;
		}
	}

	@Override
	public synchronized void remove() {
		conversionQueue.remove();
		inputIterator.remove();
	}
}
