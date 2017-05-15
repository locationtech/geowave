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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.serial.KMeansParallelInitialize;
import mil.nga.giat.geowave.analytic.kmeans.serial.StatsMap;
import mil.nga.giat.geowave.analytic.kmeans.serial.AnalyticStats.StatValue;
import mil.nga.giat.geowave.analytic.sample.BahmanEtAlSampleProbabilityFn;
import mil.nga.giat.geowave.analytic.sample.Sampler;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class KMeansParallelInitializeTest
{
	final KMeansParallelInitialize<SimpleFeature> initializer = new KMeansParallelInitialize<SimpleFeature>();
	final SimpleFeatureItemWrapperFactory itemFactory = new SimpleFeatureItemWrapperFactory();

	@Before
	public void setup() {
		initializer.getCentroidAssociationFn().setDistanceFunction(
				new FeatureCentroidDistanceFn());
		initializer.setCentroidFactory(new SimpleFeatureItemWrapperFactory());
		final Sampler<SimpleFeature> sampler = initializer.getSampler();
		sampler.setSampleProbabilityFn(new BahmanEtAlSampleProbabilityFn());
		sampler.setSampleSize(5);
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}

	@Test
	public void test() {
		final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
				initializer.getCentroidAssociationFn().getDistanceFunction(),
				getBuilder());
		final List<SimpleFeature> pointSet = dataGenerator.generatePointSet(
				0.15,
				0.2,
				10,
				10000);
		// Sort the data as if coming out of geowave
		// Also, the pointSet from the generator contains the centers first, so
		// the data is already
		// skewed to optimal sampling
		Collections.sort(
				pointSet,
				new Comparator<SimpleFeature>() {
					@Override
					public int compare(
							final SimpleFeature arg0,
							final SimpleFeature arg1 ) {
						final double arg0ToCorner = initializer
								.getCentroidAssociationFn()
								.getDistanceFunction()
								.measure(
										arg0,
										dataGenerator.getCorner());
						final double arg1ToCorner = initializer
								.getCentroidAssociationFn()
								.getDistanceFunction()
								.measure(
										arg1,
										dataGenerator.getCorner());
						return (arg0ToCorner - arg1ToCorner) < 0 ? -1 : 1;
					}
				});
		final List<AnalyticItemWrapper<SimpleFeature>> itemSet = new ArrayList<AnalyticItemWrapper<SimpleFeature>>();
		for (SimpleFeature feature : pointSet)
			itemSet.add(itemFactory.create(feature));
		final Pair<List<CentroidPairing<SimpleFeature>>, List<AnalyticItemWrapper<SimpleFeature>>> result = initializer
				.runLocal(itemSet);
		assertTrue(result.getRight().size() >= 5);
		assertTrue(isMonotonic((StatsMap) initializer.getStats()));
		for (AnalyticItemWrapper<SimpleFeature> centroid : result.getRight()) {
			System.out.println(centroid.getWrappedItem().toString() + " = " + centroid.getAssociationCount());
		}
	}

	private boolean isMonotonic(
			final StatsMap stats ) {
		Double last = null;
		for (final Double stat : stats.getStats(StatValue.COST)) {
			System.out.println(stat);
			if (last == null) {
				last = stat;
			}
			else if (last.compareTo(stat) < 0) {
				return false;
			}
		}
		return true;
	}
}
