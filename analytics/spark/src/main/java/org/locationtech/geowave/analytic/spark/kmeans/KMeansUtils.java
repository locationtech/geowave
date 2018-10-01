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

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.adapter.vector.utils.DateUtilities;
import org.locationtech.geowave.adapter.vector.utils.PolygonAreaCalculator;
import org.locationtech.geowave.core.geotime.GeometryUtils;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.ScaledTemporalRange;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.DataStore;
import org.locationtech.geowave.core.store.IndexWriter;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import scala.Tuple2;

public class KMeansUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansUtils.class);

	public static DataAdapter writeClusterCentroids(
			final KMeansModel clusterModel,
			final DataStorePluginOptions outputDataStore,
			final String centroidAdapterName,
			final ScaledTemporalRange scaledRange ) {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(centroidAdapterName);
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);

		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException fex) {
			LOGGER.error(
					fex.getMessage(),
					fex);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		if (scaledRange != null) {
			typeBuilder.add(attrBuilder.binding(
					Date.class).nillable(
					false).buildDescriptor(
					"Time"));
		}

		typeBuilder.add(attrBuilder.binding(
				Integer.class).nillable(
				false).buildDescriptor(
				"ClusterIndex"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider()
				.createPrimaryIndex(new SpatialOptions());

		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {
			for (final Vector center : clusterModel.clusterCenters()) {
				int index = clusterModel.predict(center);

				final double lon = center.apply(0);
				final double lat = center.apply(1);

				sfBuilder.set(
						Geometry.class.getName(),
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								lon,
								lat)));

				if (scaledRange != null && center.size() > 2) {
					final double timeVal = center.apply(2);

					Date time = scaledRange.valueToTime(timeVal);

					sfBuilder.set(
							"Time",
							time);

					LOGGER.warn("Write time: " + time);
				}

				sfBuilder.set(
						"ClusterIndex",
						index);

				final SimpleFeature sf = sfBuilder.buildFeature("Centroid-" + index);

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		return featureAdapter;
	}

	public static DataAdapter writeClusterHulls(
			final JavaRDD<Vector> inputCentroids,
			final KMeansModel clusterModel,
			final DataStorePluginOptions outputDataStore,
			final String hullAdapterName,
			final boolean computeMetadata ) {
		
		LOGGER.warn("Kmeans hull generation: grouping by index");
		final JavaPairRDD<Integer, Iterable<Vector>> groupByRdd = KMeansHullGenerator.groupByIndex(
				inputCentroids,
				clusterModel);

		LOGGER.warn("Kmeans hull generation: generating hulls rdd");
		final JavaPairRDD<Integer, Geometry> hullRdd = KMeansHullGenerator.generateHullsRDD(groupByRdd);

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(hullAdapterName);
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		typeBuilder.add(attrBuilder.binding(
				Integer.class).nillable(
				false).buildDescriptor(
				"ClusterIndex"));

		typeBuilder.add(attrBuilder.binding(
				Integer.class).nillable(
				false).buildDescriptor(
				"Count"));

		typeBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Area"));

		typeBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Density"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider()
				.createPrimaryIndex(new SpatialOptions());

		PolygonAreaCalculator polyCalc = (computeMetadata ? new PolygonAreaCalculator() : null);

		LOGGER.warn("Creating hull index writer...");
		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {

			LOGGER.warn("Getting hulls from rdd...");
			for (final Tuple2<Integer, Geometry> hull : hullRdd.collect()) {
				Integer index = hull._1;
				Geometry geom = hull._2;

				sfBuilder.set(
						Geometry.class.getName(),
						geom);

				sfBuilder.set(
						"ClusterIndex",
						index);

				int count = 0;
				double area = 0.0;
				double density = 0.0;

				if (computeMetadata) {
					LOGGER.warn("computing metadata for hull...");
					for (Iterable<Vector> points : groupByRdd.lookup(index)) {
						Vector[] pointVec = Iterables.toArray(
								points,
								Vector.class);
						count += pointVec.length;
					}

					try {
						// HP Fortify "NULL Pointer Dereference" false positive
						// Exception handling will catch if polyCalc is null
						area = polyCalc.getAreaDensify(geom);

						density = (double) count / area;
					}
					catch (Exception e) {
						LOGGER.error("Problem computing polygon area: " + e.getMessage());
					}
				}

				sfBuilder.set(
						"Count",
						count);

				sfBuilder.set(
						"Area",
						area);

				sfBuilder.set(
						"Density",
						density);

				LOGGER.warn("building simple feature for hull...");
				final SimpleFeature sf = sfBuilder.buildFeature("Hull-" + index);

				LOGGER.warn("writing the hull feature...");
				writer.write(sf);
				LOGGER.warn("feature write complete...");
			}
			LOGGER.warn("all hulls written...");
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		return featureAdapter;
	}

	public static ScaledTemporalRange setRunnerTimeParams(
			final KMeansRunner runner,
			final DataStorePluginOptions inputDataStore,
			ByteArrayId adapterId ) {
		if (adapterId == null) { // if no id provided, locate a single
									// featureadapter
			List<ByteArrayId> adapterIdList = FeatureDataUtils.getFeatureAdapterIds(inputDataStore);
			if (adapterIdList.size() == 1) {
				adapterId = adapterIdList.get(0);
			}
			else if (adapterIdList.isEmpty()) {
				LOGGER.error("No feature adapters found for use with time param");

				return null;
			}
			else {
				LOGGER.error("Multiple feature adapters found for use with time param. Please specify one.");

				return null;

			}
		}

		ScaledTemporalRange scaledRange = new ScaledTemporalRange();

		String timeField = FeatureDataUtils.getTimeField(
				inputDataStore,
				adapterId);

		if (timeField != null) {
			TemporalRange timeRange = DateUtilities.getTemporalRange(
					inputDataStore,
					adapterId,
					timeField);

			if (timeRange != null) {
				scaledRange.setTimeRange(
						timeRange.getStartTime(),
						timeRange.getEndTime());
			}

			String geomField = FeatureDataUtils.getGeomField(
					inputDataStore,
					adapterId);

			Envelope bbox = org.locationtech.geowave.adapter.vector.utils.FeatureGeometryUtils.getGeoBounds(
					inputDataStore,
					adapterId,
					geomField);

			if (bbox != null) {
				double xRange = bbox.getMaxX() - bbox.getMinX();
				double yRange = bbox.getMaxY() - bbox.getMinY();
				double valueRange = Math.min(
						xRange,
						yRange);
				scaledRange.setValueRange(
						0.0,
						valueRange);
			}

			runner.setTimeParams(
					timeField,
					scaledRange);

			return scaledRange;
		}

		LOGGER.error("Couldn't determine field to use for time param");

		return null;
	}
}
