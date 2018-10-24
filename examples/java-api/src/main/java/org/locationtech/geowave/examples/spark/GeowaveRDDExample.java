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
package org.locationtech.geowave.examples.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeowaveRDDExample
{
	public GeowaveRDDExample() {}

	public boolean loadRddFromStore(
			final String[] args ) {
		if (args.length < 1) {
			System.err.println("Missing required arg 'storename'");
			return false;
		}

		final String storeName = args[0];

		int minSplits = -1;
		int maxSplits = -1;
		QueryConstraints query = null;

		if (args.length > 1) {
			if (args[1].equals("--splits")) {
				if (args.length < 4) {
					System.err.println("USAGE: storename --splits min max");
					return false;
				}

				minSplits = Integer.parseInt(args[2]);
				maxSplits = Integer.parseInt(args[3]);

				if (args.length > 4) {
					if (args[4].equals("--bbox")) {
						if (args.length < 9) {
							System.err.println("USAGE: storename --splits min max --bbox west south east north");
							return false;
						}

						final double west = Double.parseDouble(args[5]);
						final double south = Double.parseDouble(args[6]);
						final double east = Double.parseDouble(args[7]);
						final double north = Double.parseDouble(args[8]);

						final Geometry bbox = new GeometryFactory().toGeometry(new Envelope(
								west,
								south,
								east,
								north));

						query = new SpatialQuery(
								bbox);
					}
				}
			}
			else if (args[1].equals("--bbox")) {
				if (args.length < 6) {
					System.err.println("USAGE: storename --bbox west south east north");
					return false;
				}

				final double west = Double.parseDouble(args[2]);
				final double south = Double.parseDouble(args[3]);
				final double east = Double.parseDouble(args[4]);
				final double north = Double.parseDouble(args[5]);

				final Geometry bbox = new GeometryFactory().toGeometry(new Envelope(
						west,
						south,
						east,
						north));

				query = new SpatialQuery(
						bbox);
			}
			else {
				System.err.println("USAGE: storename --splits min max --bbox west south east north");
				return false;
			}
		}

		try {
			DataStorePluginOptions inputStoreOptions = null;

			final StoreLoader inputStoreLoader = new StoreLoader(
					storeName);
			if (!inputStoreLoader.loadFromConfig(ConfigOptions.getDefaultPropertyFile())) {
				throw new IOException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();

			final SparkConf sparkConf = new SparkConf();

			sparkConf.setAppName("GeoWaveRDD");
			sparkConf.setMaster("local");
			final JavaSparkContext context = new JavaSparkContext(
					sparkConf);
			final RDDOptions rddOpts = new RDDOptions();
			rddOpts.setQuery(QueryBuilder.newBuilder().constraints(
					query).build());
			rddOpts.setMinSplits(minSplits);
			rddOpts.setMaxSplits(maxSplits);
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDDLoader.loadRDD(
					context.sc(),
					inputStoreOptions,
					rddOpts).getRawRDD();

			System.out.println("DataStore " + storeName + " loaded into RDD with " + javaRdd.count() + " features.");

			context.close();
		}
		catch (final IOException e) {
			System.err.println(e.getMessage());
		}

		return true;
	}
}
