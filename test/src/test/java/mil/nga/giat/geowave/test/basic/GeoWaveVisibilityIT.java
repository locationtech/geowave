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
//                            BasicAccumuloOperations ops = new BasicAccumuloOperations(zookeeper, accumuloInstance, accumuloUser, accumuloPassword, "");
//                            ops.insureAuthorization(accumuloUser, "a","b","c");

package mil.nga.giat.geowave.test.basic;

import java.io.IOException;
import java.util.Arrays;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveVisibilityIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;
	// because there are 8 we want to make
	// sure it is properly truncated
	private static final int TOTAL_FEATURES = 800;

	@Test
	public void testIngestAndQueryMixedVisibilityFields()
			throws MismatchedIndexToAdapterMapping,
			IOException {
		final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(
				getType());
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				getType());
		final DataStore store = dataStore.createDataStore();
		try (IndexWriter writer = store.createWriter(
				adapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			for (int i = 0; i < TOTAL_FEATURES; i++) {
				bldr.set(
						"a",
						Integer.toString(i));
				bldr.set(
						"b",
						Integer.toString(i));
				bldr.set(
						"c",
						Integer.toString(i));
				bldr.set(
						"geometry",
						new GeometryFactory().createPoint(new Coordinate(
								0,
								0)));
				writer.write(
						bldr.buildFeature(Integer.toString(i)),
						new VisibilityWriter<SimpleFeature>() {

							@Override
							public FieldVisibilityHandler<SimpleFeature, Object> getFieldVisibilityHandler(
									final ByteArrayId fieldId ) {
								return new FieldVisibilityHandler<SimpleFeature, Object>() {

									@Override
									public byte[] getVisibility(
											final SimpleFeature rowValue,
											final ByteArrayId fieldId,
											final Object fieldValue ) {

										final boolean isGeom = fieldId
												.equals(GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
										final int fieldValueInt;
										if (isGeom) {
											fieldValueInt = Integer.parseInt(rowValue.getID());
										}
										else {
											fieldValueInt = Integer.parseInt(fieldValue.toString());
										}
										// just make half of them varied and
										// half of them the same
										if ((fieldValueInt % 2) == 0) {
											if (isGeom) {
												return new byte[] {};
											}
											return fieldId.getBytes();
										}
										else {
											// of the ones that are the same,
											// make some no bytes, some a, some
											// b, and some c
											final int switchValue = (fieldValueInt / 2) % 4;
											switch (switchValue) {
												case 0:
													return new ByteArrayId(
															"a").getBytes();

												case 1:
													return new ByteArrayId(
															"b").getBytes();

												case 2:
													return new ByteArrayId(
															"c").getBytes();

												case 3:
												default:
													return new byte[] {};
											}
										}
									}
								};

							}
						});
			}
		}
		final DifferingFieldVisibilityEntryCount differingVisibilities = (DifferingFieldVisibilityEntryCount) dataStore
				.createDataStatisticsStore()
				.getDataStatistics(
						adapter.getAdapterId(),
						DifferingFieldVisibilityEntryCount.composeId(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		Assert.assertEquals(
				"Exactly half the entries should have differing visibility",
				TOTAL_FEATURES / 2,
				differingVisibilities.getEntriesWithDifferingFieldVisibilities());
		testQuery(
				store,
				false);
		testQuery(
				store,
				true);
	}

	private static void testQuery(
			final DataStore store,
			boolean spatial )
			throws IOException {

		// you have to at least be able to see the geometry field which is wide
		// open for exactly (5 * total_Features / 8)
		// for other fields there is exactly
		testQuery(
				store,
				new String[] {},
				spatial,
				(5 * TOTAL_FEATURES) / 8,
				((TOTAL_FEATURES / 8) * 4) + (TOTAL_FEATURES / 2));
		for (String auth : new String[] {
			"a",
			"b",
			"c"
		}) {
			testQuery(
					store,
					new String[] {
						auth
					},
					spatial,
					(6 * TOTAL_FEATURES) / 8,
					((2 * TOTAL_FEATURES / 8) * 4) + (2 * TOTAL_FEATURES / 2));
		}
		// order shouldn't matter, but let's make sure here
		for (String[] auths : new String[][] {
			new String[] {
				"a",
				"b"
			},
			new String[] {
				"b",
				"a"
			},
			new String[] {
				"a",
				"c"
			},
			new String[] {
				"c",
				"a"
			},
			new String[] {
				"b",
				"c"
			},
			new String[] {
				"c",
				"b"
			}
		}) {
			testQuery(
					store,
					auths,
					spatial,
					(7 * TOTAL_FEATURES) / 8,
					((3 * TOTAL_FEATURES / 8) * 4) + (3 * TOTAL_FEATURES / 2));
		}

		testQuery(
				store,
				new String[] {
					"a",
					"b",
					"c"
				},
				spatial,
				TOTAL_FEATURES,
				TOTAL_FEATURES * 4);
	}

	private static void testQuery(
			final DataStore store,
			final String[] auths,
			final boolean spatial,
			final int expectedResultCount,
			final int expectedNonNullFieldCount )
			throws IOException {
		final QueryOptions queryOpts = new QueryOptions();
		queryOpts.setAuthorizations(auths);
		try (CloseableIterator<SimpleFeature> it = (CloseableIterator) store.query(
				queryOpts,
				spatial ? new SpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								-1,
								1,
								-1,
								1))) : null)) {
			int resultCount = 0;
			int nonNullFieldsCount = 0;
			while (it.hasNext()) {
				final SimpleFeature feature = it.next();
				for (int a = 0; a < feature.getAttributeCount(); a++) {
					if (feature.getAttribute(a) != null) {
						nonNullFieldsCount++;
					}
				}
				resultCount++;
			}
			Assert.assertEquals(
					"Unexpected result count for " + (spatial ? "spatial query" : "full table scan") + " with auths "
							+ Arrays.toString(auths),
					expectedResultCount,
					resultCount);

			Assert.assertEquals(
					"Unexpected non-null field count for " + (spatial ? "spatial query" : "full table scan")
							+ " with auths " + Arrays.toString(auths),
					expectedNonNullFieldCount,
					nonNullFieldsCount);
		}

		queryOpts.setAggregation(
				new CountAggregation(),
				new FeatureDataAdapter(
						getType()));
		try (CloseableIterator<CountResult> it = (CloseableIterator) store.query(
				queryOpts,
				spatial ? new SpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								-1,
								1,
								-1,
								1))) : null)) {
			CountResult result = it.next();
			long count = 0;
			if (result != null) {
				count = result.getCount();
			}
			Assert.assertEquals(
					"Unexpected aggregation result count for " + (spatial ? "spatial query" : "full table scan")
							+ " with auths " + Arrays.toString(auths),
					expectedResultCount,
					count);
		}
	}

	private static SimpleFeatureType getType() {
		final SimpleFeatureTypeBuilder bldr = new SimpleFeatureTypeBuilder();
		bldr.setName("testvis");
		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"a"));
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"b"));
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"c"));
		bldr.add(attributeTypeBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		return bldr.buildFeatureType();
	}
}
