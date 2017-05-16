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
package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;

/**
 * This class is an Accumulo Iterator that can support skipping by a fixed
 * cardinality on a Space Filling Curve (skipping by incrementing a fixed bit
 * position of the row ID).
 * 
 */
public class FixedCardinalitySkippingIterator extends
		SkippingIterator implements
		InterruptibleIterator
{
	protected static final String CARDINALITY_SKIPPING_ITERATOR_NAME = "CARDINALITY_SKIPPING_ITERATOR";
	protected static final int CARDINALITY_SKIPPING_ITERATOR_PRIORITY = 35;
	protected static final String CARDINALITY_SKIP_INTERVAL = "cardinality";
	protected Text nextRow;
	protected Integer bitPosition;
	protected Collection<ByteSequence> columnFamilies;
	private boolean reachedEnd = false;

	protected boolean inclusive = false;
	protected Range range;

	public FixedCardinalitySkippingIterator() {
		super();
	}

	public FixedCardinalitySkippingIterator(
			final SortedKeyValueIterator<Key, Value> source ) {
		setSource(source);
	}

	protected FixedCardinalitySkippingIterator(
			final SortedKeyValueIterator<Key, Value> source,
			final Integer bitPosition,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive ) {
		this(
				source);
		this.columnFamilies = columnFamilies;
		this.bitPosition = bitPosition;
		this.inclusive = inclusive;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		final String bitPositionStr = options.get(CARDINALITY_SKIP_INTERVAL);
		if (bitPositionStr == null) {
			throw new IllegalArgumentException(
					"'precision' must be set for " + FixedCardinalitySkippingIterator.class.getName());
		}
		try {
			bitPosition = Integer.parseInt(bitPositionStr);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					"Unable to parse value",
					e);
		}
		super.init(
				source,
				options,
				env);
	}

	@Override
	public void next()
			throws IOException {
		final byte[] nextRowBytes = incrementBit(getTopKey().getRow().getBytes());

		if (nextRowBytes == null) {
			reachedEnd = true;
		}
		else {
			nextRow = new Text(
					nextRowBytes);
		}
		super.next();
	}

	@Override
	public Key getTopKey() {
		if (reachedEnd) {
			return null;
		}
		return super.getTopKey();
	}

	@Override
	public Value getTopValue() {
		if (reachedEnd) {
			return null;
		}
		return super.getTopValue();
	}

	@Override
	public boolean hasTop() {
		if (reachedEnd) {
			return false;
		}
		return super.hasTop();
	}

	private byte[] incrementBit(
			final byte[] row ) {
		final int cardinality = bitPosition + 1;
		final byte[] rowCopy = new byte[(int) Math.ceil(cardinality / 8.0)];
		System.arraycopy(
				row,
				0,
				rowCopy,
				0,
				rowCopy.length);
		// number of bits not used in the last byte
		int remainder = (8 - (cardinality % 8));
		if (remainder == 8) {
			remainder = 0;
		}

		final int numIncrements = (int) Math.pow(
				2,
				remainder);
		if (remainder > 0) {
			for (int i = 0; i < remainder; i++) {
				rowCopy[rowCopy.length - 1] |= (1 << (i));
			}
		}
		for (int i = 0; i < numIncrements; i++) {
			if (!ByteArrayUtils.increment(rowCopy)) {
				return null;
			}
		}
		return rowCopy;
	}

	@Override
	protected void consume()
			throws IOException {
		while (getSource().hasTop() && ((nextRow != null) && (getSource().getTopKey().getRow().compareTo(
				nextRow) < 0))) {
			// seek to the next column family in the sorted list of
			// column families
			reseek(new Key(
					nextRow));
		}
	}

	private void reseek(
			final Key key )
			throws IOException {
		if (range.afterEndKey(key)) {
			range = new Range(
					range.getEndKey(),
					true,
					range.getEndKey(),
					range.isEndKeyInclusive());
			getSource().seek(
					range,
					columnFamilies,
					inclusive);
		}
		else {
			range = new Range(
					key,
					true,
					range.getEndKey(),
					range.isEndKeyInclusive());
			getSource().seek(
					range,
					columnFamilies,
					inclusive);
		}
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		return new FixedCardinalitySkippingIterator(
				getSource().deepCopy(
						env),
				bitPosition,
				columnFamilies,
				inclusive);
	}

	@Override
	public void seek(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive )
			throws IOException {
		this.range = range;
		this.columnFamilies = columnFamilies;
		this.inclusive = inclusive;
		reachedEnd = false;
		super.seek(
				range,
				columnFamilies,
				inclusive);
	}

	@Override
	public void setInterruptFlag(
			final AtomicBoolean flag ) {
		((InterruptibleIterator) getSource()).setInterruptFlag(flag);
	}
}
