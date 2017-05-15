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
package mil.nga.giat.geowave.analytic.sample;

import java.util.Collection;
import java.util.SortedMap;

import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;

import com.google.common.collect.Maps;

public class Sampler<T>
{
	private int sampleSize = 1;
	private int putLimit = 100000;
	private SampleProbabilityFn sampleProbabilityFn;

	public SampleProbabilityFn getSampleProbabilityFn() {
		return sampleProbabilityFn;
	}

	public void setSampleProbabilityFn(
			final SampleProbabilityFn sampleProbabilityFn ) {
		this.sampleProbabilityFn = sampleProbabilityFn;
	}

	public int getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(
			final int sampleSize ) {
		this.sampleSize = sampleSize;
	}

	public int getPutLimit() {
		return putLimit;
	}

	public void setPutLimit(
			final int putLimit ) {
		this.putLimit = putLimit;
	}

	public void sample(
			final Iterable<CentroidPairing<T>> pairings,
			final SampleNotification<T> notification,
			final double normalizingConstant ) {
		int putCounter = 0;

		final SortedMap<Double, T> reservoir = Maps.newTreeMap();
		for (final CentroidPairing<T> pairing : pairings) {
			final double weight = pairing.getDistance();
			if (weight > 0.0) {
				final double score = sampleProbabilityFn.getProbability(
						weight,
						normalizingConstant,
						sampleSize);
				// could add extra to make sure new point is far enough away
				// from the rest
				if (reservoir.size() < sampleSize) {
					reservoir.put(
							score,
							pairing.getPairedItem().getWrappedItem());
					putCounter++;
				}
				else if (score > reservoir.firstKey()) {
					reservoir.remove(reservoir.firstKey());
					reservoir.put(
							score,
							pairing.getPairedItem().getWrappedItem());
				}
				if (putCounter > putLimit) {
					// On the off-chance this gets huge, cleanup
					// Can occur if sampleSize > PUT_LIMIT
					notifyAll(
							notification,
							reservoir.values(),
							true);
					reservoir.clear();
					putCounter = 0;
				}
			}
		}
		notifyAll(
				notification,
				reservoir.values(),
				false);
	}

	private void notifyAll(
			final SampleNotification<T> notification,
			final Collection<T> items,
			final boolean partial ) {
		for (final T item : items) {
			notification.notify(
					item,
					partial);
		}
	}

}
