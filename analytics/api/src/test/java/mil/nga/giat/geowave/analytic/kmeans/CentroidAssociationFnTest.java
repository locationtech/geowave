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
package mil.nga.giat.geowave.analytic.kmeans;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.clustering.LongCentroid;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.kmeans.CentroidAssociationFn;

import org.junit.Assert;
import org.junit.Test;

public class CentroidAssociationFnTest
{

	private static Set<CentroidPairing<Long>> expectedPairings = new HashSet<CentroidPairing<Long>>();
	private static double expectedCost = 0;
	static {
		expectedPairings.add(new CentroidPairing<Long>(
				new LongCentroid(
						10,
						"",
						0),
				new LongCentroid(
						345,
						"",
						0),
				335));
		expectedPairings.add(new CentroidPairing<Long>(
				new LongCentroid(
						1000,
						"",
						0),
				new LongCentroid(
						764,
						"",
						0),
				236));
		expectedPairings.add(new CentroidPairing<Long>(
				new LongCentroid(
						10,
						"",
						0),
				new LongCentroid(
						89,
						"",
						0),
				79));
		expectedPairings.add(new CentroidPairing<Long>(
				new LongCentroid(
						1000,
						"",
						0),
				new LongCentroid(
						900,
						"",
						0),
				100));
		for (final CentroidPairing<Long> pairing : expectedPairings) {
			expectedCost += pairing.getDistance();
		}
	}

	@Test
	public void test() {
		final CentroidAssociationFn<Long> fn = new CentroidAssociationFn<Long>();
		fn.setDistanceFunction(new DistanceFn<Long>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public double measure(
					final Long x,
					final Long y ) {
				return Math.abs(x.longValue() - y.longValue());
			}
		});
		final List<AnalyticItemWrapper<Long>> dataSet = Arrays.asList(
				(AnalyticItemWrapper<Long>) new LongCentroid(
						345,
						"",
						0),
				new LongCentroid(
						764,
						"",
						0),
				new LongCentroid(
						89,
						"",
						0),
				new LongCentroid(
						900,
						"",
						0));
		final List<AnalyticItemWrapper<Long>> centroidSet = Arrays.asList(
				(AnalyticItemWrapper<Long>) new LongCentroid(
						10,
						"",
						0),
				(AnalyticItemWrapper<Long>) new LongCentroid(
						1000,
						"",
						0));
		final double cost = fn.compute(
				dataSet,
				centroidSet,
				new AssociationNotification<Long>() {

					@Override
					public void notify(
							final CentroidPairing<Long> pairing ) {
						Assert.assertTrue(expectedPairings.contains(pairing));
					}
				});
		Assert.assertEquals(
				expectedCost,
				cost,
				0.0001);
	}

}
