package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.UUID;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.vector.utils.DateUtilities;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class GeoWaveFeatureSourceTest
{
	GeoWaveGTMemDataStore dataStore;
	SimpleFeatureType schema;
	SimpleFeatureType type;
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException {
		dataStore = new GeoWaveGTMemDataStore();
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");

		dataStore.createSchema(type);

	}

	@Test
	public void testEmpty()
			throws IOException {
		SimpleFeatureSource source = dataStore.getFeatureSource("geostuff");
		ReferencedEnvelope env = source.getBounds();
		assertEquals(
				90.0,
				env.getMaxX(),
				0.0001);
		assertEquals(
				-180.0,
				env.getMinY(),
				0.0001);
		Query query = new Query(
				"geostuff",
				Filter.INCLUDE);
		assertEquals(
				0,
				source.getCount(query));
	}

	@Test
	public void testFull()
			throws Exception {
		populate();
		SimpleFeatureSource source = dataStore.getFeatureSource("geostuff");
		ReferencedEnvelope env = source.getBounds();
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
		Query query = new Query(
				"geostuff",
				Filter.INCLUDE);
		assertEquals(
				3,
				source.getCount(query));

		CloseableIterator<DataStatistics<?>> stats = dataStore.dataStore.getStatsStore().getDataStatistics(
				new ByteArrayId(
						"geostuff".getBytes()));
		assertTrue(stats.hasNext());
		DataStatistics<?> stats1 = stats.next();
		assertTrue(stats.hasNext());
		DataStatistics<?> stats2 = stats.next();
		assertFalse(stats.hasNext());
		BoundingBoxDataStatistics<SimpleFeature> bboxStats;
		CountDataStatistics<SimpleFeature> cStats;
		if (stats1 instanceof BoundingBoxDataStatistics) {

			bboxStats = (BoundingBoxDataStatistics<SimpleFeature>) stats1;
			cStats = (CountDataStatistics<SimpleFeature>) stats2;
		}
		else {

			bboxStats = (BoundingBoxDataStatistics<SimpleFeature>) stats2;
			cStats = (CountDataStatistics<SimpleFeature>) stats1;
		}
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

	@Test
	public void testPartial()
			throws CQLException,
			IOException,
			ParseException {

		populate();
		SimpleFeatureSource source = dataStore.getFeatureSource("geostuff");

		Query query = new Query(
				"geostuff",
				CQL.toFilter("BBOX(geometry,42,28,44,30) and when during 2005-05-01T20:32:56Z/2005-05-29T21:32:56Z"),
				new String[] {
					"geometry",
					"pid"
				});
		ReferencedEnvelope env = source.getBounds(query);
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

	private void populate()
			throws IOException,
			CQLException,
			ParseException {
		Transaction transaction1 = new DefaultTransaction();

		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				Filter.EXCLUDE,
				transaction1);
		assertFalse(writer.hasNext());
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				new Long(
						100));
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
				new Long(
						100));
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
				new Long(
						100));
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
