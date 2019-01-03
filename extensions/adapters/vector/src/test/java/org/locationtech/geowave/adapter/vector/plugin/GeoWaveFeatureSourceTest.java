/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership. All rights reserved. This program and the accompanying materials are made available under the terms of the Apache License, Version 2.0 which accompanies this distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.UUID;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.BaseDataStoreTest;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

public class GeoWaveFeatureSourceTest extends
		BaseDataStoreTest
{
	static final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Test
	public void test()
			throws Exception {
		testEmpty();
		testFull(
				new FWPopulater(),
				"fw");
		testPartial(
				new FWPopulater(),
				"fw");
		// test different populate methods
		testFull(
				new SourcePopulater(),
				"s");
		testPartial(
				new SourcePopulater(),
				"s");
	}

	public void testEmpty()
			throws Exception {
		final SimpleFeatureType type = DataUtilities.createType(
				"GeoWaveFeatureSourceTest_e",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");
		final DataStore dataStore = createDataStore();
		dataStore.createSchema(type);
		final SimpleFeatureSource source = dataStore.getFeatureSource("GeoWaveFeatureSourceTest_e");
		final ReferencedEnvelope env = source.getBounds();
		assertEquals(
				90.0,
				env.getMaxX(),
				0.0001);
		assertEquals(
				-180.0,
				env.getMinY(),
				0.0001);
		final Query query = new Query(
				"GeoWaveFeatureSourceTest_e",
				Filter.INCLUDE);
		assertEquals(
				0,
				source.getCount(query));
	}

	public void testFull(
			final Populater populater,
			final String ext )
			throws Exception {
		final String typeName = "GeoWaveFeatureSourceTest_full" + ext;
		final SimpleFeatureType type = DataUtilities.createType(
				typeName,
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");
		final DataStore dataStore = createDataStore();
		populater.populate(
				type,
				dataStore);
		final SimpleFeatureSource source = dataStore.getFeatureSource(typeName);
		final ReferencedEnvelope env = source.getBounds();
		assertEquals(
				43.454,
				env.getMaxX(),
				0.0001);
		assertEquals(
				27.232,
				env.getMinY(),
				0.0001);
		assertEquals(
				28.242,
				env.getMaxY(),
				0.0001);
		final Query query = new Query(
				typeName,
				Filter.INCLUDE);
		assertTrue(source.getCount(query) > 2);

		final short internalAdapterId = ((GeoWaveGTDataStore) dataStore).getInternalAdapterStore().addTypeName(
				typeName);
		try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> stats = ((GeoWaveGTDataStore) dataStore)
				.getDataStatisticsStore()
				.getDataStatistics(
						internalAdapterId)) {
			assertTrue(stats.hasNext());
			int count = 0;
			BoundingBoxDataStatistics<SimpleFeature> bboxStats = null;
			CountDataStatistics<SimpleFeature> cStats = null;
			FeatureTimeRangeStatistics timeRangeStats = null;
			FeatureNumericRangeStatistics popStats = null;
			while (stats.hasNext()) {
				final InternalDataStatistics<?, ?, ?> statsData = stats.next();
				if (statsData instanceof BoundingBoxDataStatistics) {
					bboxStats = (BoundingBoxDataStatistics<SimpleFeature>) statsData;
				}
				else if (statsData instanceof CountDataStatistics) {
					cStats = (CountDataStatistics<SimpleFeature>) statsData;
				}
				else if (statsData instanceof FeatureTimeRangeStatistics) {
					timeRangeStats = (FeatureTimeRangeStatistics) statsData;
				}
				else if (statsData instanceof FeatureNumericRangeStatistics) {
					popStats = (FeatureNumericRangeStatistics) statsData;
				}
				count++;
			}
			// rather than maintain an exact count on stats as we should be able
			// to add them more dynamically, just make sure that there is some
			// set of base stats found
			assertTrue(
					"Unexpectedly few stats found",
					count > 5);

			assertEquals(
					66,
					popStats.getMin(),
					0.001);
			assertEquals(
					100,
					popStats.getMax(),
					0.001);
			assertEquals(
					DateUtilities.parseISO("2005-05-17T20:32:56Z"),
					timeRangeStats.asTemporalRange().getStartTime());
			assertEquals(
					DateUtilities.parseISO("2005-05-19T20:32:56Z"),
					timeRangeStats.asTemporalRange().getEndTime());
			assertEquals(
					43.454,
					bboxStats.getMaxX(),
					0.0001);
			assertEquals(
					27.232,
					bboxStats.getMinY(),
					0.0001);
			assertEquals(
					3,
					cStats.getCount());
		}

	}

	public void testPartial(
			final Populater populater,
			final String ext )
			throws CQLException,
			Exception {
		final String typeName = "GeoWaveFeatureSourceTest_p" + ext;
		final SimpleFeatureType type = DataUtilities.createType(
				typeName,
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");
		final DataStore dataStore = createDataStore();
		populater.populate(
				type,
				dataStore);
		final SimpleFeatureSource source = dataStore.getFeatureSource(typeName);

		final Query query = new Query(
				typeName,
				CQL.toFilter("BBOX(geometry,42,28,44,30) and when during 2005-05-01T20:32:56Z/2005-05-29T21:32:56Z"),
				new String[] {
					"geometry",
					"when",
					"pid"
				});
		final ReferencedEnvelope env = source.getBounds(query);
		assertEquals(
				43.454,
				env.getMaxX(),
				0.0001);
		assertEquals(
				28.232,
				env.getMinY(),
				0.0001);
		assertEquals(
				28.242,
				env.getMaxY(),
				0.0001);
		assertEquals(
				2,
				source.getCount(query));

	}

	public interface Populater
	{
		void populate(
				final SimpleFeatureType type,
				final DataStore dataStore )
				throws IOException,
				CQLException,
				ParseException;
	}

	private static class FWPopulater implements
			Populater
	{
		@Override
		public void populate(
				final SimpleFeatureType type,
				final DataStore dataStore )
				throws IOException,
				CQLException,
				ParseException {

			dataStore.createSchema(type);

			final Transaction transaction1 = new DefaultTransaction();

			final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
					type.getTypeName(),
					transaction1);
			assertFalse(writer.hasNext());
			SimpleFeature newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(77));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-19T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							28.232)));
			writer.write();

			newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(66));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-18T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							27.232)));
			writer.write();

			newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(100));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-17T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							28.242)));
			writer.write();
			writer.close();
			transaction1.commit();
			transaction1.close();
		}
	}

	private static class SourcePopulater implements
			Populater
	{
		@Override
		public void populate(
				final SimpleFeatureType type,
				final DataStore dataStore )
				throws IOException,
				CQLException,
				ParseException {

			dataStore.createSchema(type);

			final Transaction transaction1 = new DefaultTransaction();

			final SimpleFeatureStore source = (SimpleFeatureStore) dataStore.getFeatureSource(type.getName());
			final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
					type.getTypeName(),
					transaction1);
			assertFalse(writer.hasNext());
			SimpleFeature newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(77));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-19T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							28.232)));
			source.addFeatures(DataUtilities.collection(newFeature));

			newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(66));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-18T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							27.232)));
			source.addFeatures(DataUtilities.collection(newFeature));

			newFeature = writer.next();
			newFeature.setAttribute(
					"pop",
					Long.valueOf(100));
			newFeature.setAttribute(
					"pid",
					UUID.randomUUID().toString());
			newFeature.setAttribute(
					"when",
					DateUtilities.parseISO("2005-05-17T20:32:56Z"));
			newFeature.setAttribute(
					"geometry",
					factory.createPoint(new Coordinate(
							43.454,
							28.242)));
			source.addFeatures(DataUtilities.collection(newFeature));
			transaction1.commit();
			transaction1.close();
		}
	}

}
