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
package org.locationtech.geowave.core.store.adapter.statistics.histogram;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.ByteArrayUtils;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

public class TDigestNumericHistogram implements
		NumericHistogram
{
	private static final double DEFAULT_COMPRESSION = 100;
	private TDigest tdigest;
	private long count = 0;

	public TDigestNumericHistogram() {
		super();
		tdigest = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
	}

	@Override
	public void merge(
			final NumericHistogram other ) {
		if (other instanceof TDigestNumericHistogram) {
			tdigest.add(((TDigestNumericHistogram) other).tdigest);
			count += ((TDigestNumericHistogram) other).count;
		}
	}

	@Override
	public void add(
			final double v ) {
		tdigest.add(v);
		count++;
	}

	@Override
	public double quantile(
			final double q ) {
		return tdigest.quantile(q);
	}

	@Override
	public double cdf(
			final double val ) {
		return tdigest.cdf(val);
	}

	@Override
	public int bufferSize() {
		return tdigest.smallByteSize() + ByteArrayUtils.variableLengthEncode(count).length;
	}

	@Override
	public void toBinary(
			final ByteBuffer buffer ) {
		tdigest.asSmallBytes(buffer);
		buffer.put(ByteArrayUtils.variableLengthEncode(count));
	}

	@Override
	public void fromBinary(
			final ByteBuffer buffer ) {
		tdigest = MergingDigest.fromBytes(buffer);
		final byte[] remaining = new byte[buffer.remaining()];
		buffer.get(remaining);
		count = ByteArrayUtils.variableLengthDecode(remaining);
	}

	@Override
	public double getMaxValue() {
		return tdigest.getMax();
	}

	@Override
	public double getMinValue() {
		return tdigest.getMin();
	}

	@Override
	public long getTotalCount() {
		return count;
	}

	@Override
	public double sum(
			final double val,
			final boolean inclusive ) {
		final double sum = tdigest.cdf(val) * count;
		if (inclusive && (sum < 1)) {
			return 1.0;
		}
		return sum;
	}

}
