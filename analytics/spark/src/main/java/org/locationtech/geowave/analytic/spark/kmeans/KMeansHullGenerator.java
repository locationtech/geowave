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
package org.locationtech.geowave.analytic.spark.kmeans;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class KMeansHullGenerator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansHullGenerator.class);

	public static JavaPairRDD<Integer, Iterable<Vector>> groupByIndex(
			final JavaRDD<Vector> inputPoints,
			final KMeansModel clusterModel ) {
		// Group the input points by their kmeans centroid index
		return inputPoints.groupBy(
				point -> {
					return clusterModel.predict(
							point);
				});
	}

	public static JavaPairRDD<Integer, Geometry> generateHullsRDD(
			final JavaPairRDD<Integer, Iterable<Vector>> groupedPoints ) {
		// Create the convex hull for each kmeans centroid
		final JavaPairRDD<Integer, Geometry> hullRDD = groupedPoints.mapValues(
				point -> {
					final Iterable<Coordinate> coordIt = Iterables.transform(
							point,
							new com.google.common.base.Function<Vector, Coordinate>() {
								@Override
								public Coordinate apply(
										final Vector input ) {
									if (input != null) {
										return new Coordinate(
												input.apply(
														0),
												input.apply(
														1));
									}

									return new Coordinate();
								}
							});

					final Coordinate[] coordArray = Iterables.toArray(
							coordIt,
							Coordinate.class);

					return new ConvexHull(
							coordArray,
							GeometryUtils.GEOMETRY_FACTORY).getConvexHull();
				});

		return hullRDD;
	}
}
