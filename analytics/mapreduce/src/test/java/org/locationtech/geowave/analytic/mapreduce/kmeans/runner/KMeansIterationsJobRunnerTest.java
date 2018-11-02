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
package org.locationtech.geowave.analytic.mapreduce.kmeans.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManager;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.opengis.feature.simple.SimpleFeature;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class KMeansIterationsJobRunnerTest
{

	private final KMeansIterationsJobRunnerForTest jobRunner = new KMeansIterationsJobRunnerForTest();
	private static final String[] grps = new String[] {
		"g1",
		"g2"
	};
	private static final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {},
			BasicFeatureTypes.DEFAULT_NAMESPACE,
			ClusteringUtils.CLUSTERING_CRS);

	PropertyManagement propertyMgt = new PropertyManagement();

	@Before
	public void setup() {
		propertyMgt.store(
				GlobalParameters.Global.BATCH_ID,
				"b1");
		propertyMgt.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				"centroid");
		propertyMgt.store(
				CentroidParameters.Centroid.INDEX_NAME,
				new SpatialDimensionalityTypeProvider().createIndex(
						new SpatialOptions()).getName());
		propertyMgt.store(
				ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE,
				new Double(
						0.0001));
		propertyMgt.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		propertyMgt.store(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
	}

	@Test
	public void testRun()
			throws Exception {
		// seed
		jobRunner.runJob(
				new Configuration(),
				propertyMgt);
		// then test
		jobRunner.run(
				new Configuration(),
				propertyMgt);

		for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> e : KMeansIterationsJobRunnerForTest.groups
				.entrySet()) {
			assertEquals(
					3,
					e.getValue().size());

			for (final AnalyticItemWrapper<SimpleFeature> newCentroid : e.getValue()) {
				assertEquals(
						2,
						newCentroid.getIterationID());
				// check to make sure there is no overlap of old and new IDs
				boolean b = false;
				for (final AnalyticItemWrapper<SimpleFeature> oldCentroid : KMeansIterationsJobRunnerForTest.deletedSet
						.get(e.getKey())) {
					b |= oldCentroid.getID().equals(
							newCentroid.getID());

				}
				assertFalse(b);
			}

		}

		for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> e : KMeansIterationsJobRunnerForTest.deletedSet
				.entrySet()) {
			assertEquals(
					3,
					e.getValue().size());
			for (final AnalyticItemWrapper<SimpleFeature> oldCentroid : e.getValue()) {
				assertEquals(
						1,
						oldCentroid.getIterationID());
			}
		}

	}

	public static class KMeansIterationsJobRunnerForTest extends
			KMeansIterationsJobRunner<SimpleFeature>
	{
		private int iteration = 1;
		protected static Map<String, List<AnalyticItemWrapper<SimpleFeature>>> groups = new HashMap<>();
		protected static Map<String, List<AnalyticItemWrapper<SimpleFeature>>> deletedSet = new HashMap<>();
		private static SimpleFeatureItemWrapperFactory factory = new SimpleFeatureItemWrapperFactory();
		private static final GeometryFactory geoFactory = new GeometryFactory();
		private static Point[] points = new Point[] {
			geoFactory.createPoint(new Coordinate(
					2.3,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.32,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.33)),
			geoFactory.createPoint(new Coordinate(
					2.29,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.3,
					2.32)),
			geoFactory.createPoint(new Coordinate(
					2.28,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.28,
					2.27)),
			geoFactory.createPoint(new Coordinate(
					2.27,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.33,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.35))
		};

		@Override
		protected CentroidManager<SimpleFeature> constructCentroidManager(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws IOException {
			return new CentroidManager<SimpleFeature>() {

				@Override
				public void clear() {

				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> createNextCentroid(
						final SimpleFeature feature,
						final String groupID,
						final Coordinate coordinate,
						final String[] extraNames,
						final double[] extraValues ) {
					return factory.createNextItem(
							feature,
							groupID,
							coordinate,
							extraNames,
							extraValues);
				}

				@Override
				public void delete(
						final String[] dataIds )
						throws IOException {
					final List<String> grps = Arrays.asList(dataIds);
					for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> entry : groups.entrySet()) {
						final Iterator<AnalyticItemWrapper<SimpleFeature>> it = entry.getValue().iterator();
						while (it.hasNext()) {
							final AnalyticItemWrapper<SimpleFeature> next = it.next();
							if (grps.contains(next.getID())) {
								deletedSet.get(
										entry.getKey()).add(
										next);
								it.remove();
							}
						}
					}
				}

				@Override
				public List<String> getAllCentroidGroups()
						throws IOException {
					final List<String> ll = new ArrayList<>();
					for (final String g : groups.keySet()) {
						ll.add(g);
					}
					return ll;
				}

				@Override
				public List<AnalyticItemWrapper<SimpleFeature>> getCentroidsForGroup(
						final String groupID )
						throws IOException {
					return groups.get(groupID);
				}

				@Override
				public List<AnalyticItemWrapper<SimpleFeature>> getCentroidsForGroup(
						final String batchID,
						final String groupID )
						throws IOException {
					return groups.get(groupID);
				}

				@Override
				public int processForAllGroups(
						final org.locationtech.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn<SimpleFeature> fn )
						throws IOException {
					for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> entry : groups.entrySet()) {
						final int status = fn.processGroup(
								entry.getKey(),
								entry.getValue());
						if (status < 0) {
							return status;
						}
					}
					return 0;
				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> getCentroid(
						final String id ) {
					// TODO Auto-generated method stub
					return null;
				}

				@Override
				public String getDataTypeName() {
					return "centroid";
				}

				@Override
				public String getIndexName() {
					return new SpatialDimensionalityTypeProvider().createIndex(
							new SpatialOptions()).getName();
				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> getCentroidById(
						final String id,
						final String groupID )
						throws IOException,
						MatchingCentroidNotFoundException {
					final Iterator<AnalyticItemWrapper<SimpleFeature>> it = this.getCentroidsForGroup(
							groupID).iterator();
					while (it.hasNext()) {
						final AnalyticItemWrapper<SimpleFeature> feature = (it.next());
						if (feature.getID().equals(
								id)) {
							return feature;
						}
					}
					throw new MatchingCentroidNotFoundException(
							id);
				}

			};
		}

		@Override
		protected int runJob(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws Exception {
			int j = 0;
			for (final String grpID : grps) {
				if (!groups.containsKey(grpID)) {
					groups.put(
							grpID,
							new ArrayList<AnalyticItemWrapper<SimpleFeature>>());
					deletedSet.put(
							grpID,
							new ArrayList<AnalyticItemWrapper<SimpleFeature>>());
				}
				for (int i = 0; i < 3; i++) {
					final SimpleFeature nextFeature = AnalyticFeature.createGeometryFeature(
							adapter.getFeatureType(),
							"b1",
							UUID.randomUUID().toString(),
							"nn" + i,
							grpID,
							0.1,
							points[j++],
							new String[0],
							new double[0],
							1,
							iteration,
							0);
					groups.get(
							grpID).add(
							factory.create(nextFeature));
				}
			}
			iteration++;
			return 0;
		}
	}

}
