package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import mil.nga.giat.geowave.vector.utils.DateUtilities;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.feature.visitor.MaxVisitor;
import org.geotools.feature.visitor.MinVisitor;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.spatial.BBOXImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class GeoWaveFeatureReaderTest
{
	GeoWaveGTMemDataStore dataStore;
	SimpleFeatureType schema;
	SimpleFeatureType type;
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));
	Query query = null;
	List<String> fids = new ArrayList<String>();
	Date stime, etime;

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			Exception {

		dataStore = new GeoWaveGTMemDataStore();
		type = DataUtilities.createType(
				"GeoWaveFeatureReaderTest",
				"geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");

		dataStore.createSchema(type);

		stime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
		etime = DateUtilities.parseISO("2005-05-20T20:32:56Z");

		final Transaction transaction1 = new DefaultTransaction();
		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertFalse(writer.hasNext());
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"start",
				stime);
		newFeature.setAttribute(
				"end",
				etime);
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		fids.add(newFeature.getID());
		writer.write();
		newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(101));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"start",
				etime);
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						28.25,
						41.25)));
		fids.add(newFeature.getID());
		writer.write();
		writer.close();
		transaction1.commit();
		transaction1.close();

		// System.out.println(fids);
		query = new Query(
				"GeoWaveFeatureReaderTest",
				ECQL.toFilter("IN ('" + fids.get(0) + "')"),
				new String[] {
					"geometry",
					"pid"
				});

	}

	@Test
	public void testBBOX()
			throws IllegalArgumentException,
			NoSuchElementException,
			IOException {
		FilterFactoryImpl factory = new FilterFactoryImpl();
		Query query = new Query(
				"GeoWaveFeatureReaderTest",
				factory.bbox(
						"",
						-180,
						-90,
						180,
						90,
						"EPSG:4326"),
				new String[] {
					"geometry",
					"pid"
				});

		final FeatureReader reader = dataStore.getFeatureReader(
				type.getTypeName(),
				query);
		int count = 0;
		while (reader.hasNext()) {
			final SimpleFeature feature = (SimpleFeature) reader.next();
			assertTrue(fids.contains(feature.getID()));
			count++;
		}
		assertTrue(count > 0);

	}

	@Test
	public void testRangeIndex()
			throws IllegalArgumentException,
			NoSuchElementException,
			IOException {
		final FeatureReader reader = dataStore.getFeatureReader(
				type.getTypeName(),
				query);
		int count = 0;
		while (reader.hasNext()) {
			final SimpleFeature feature = (SimpleFeature) reader.next();
			assertTrue(fids.contains(feature.getID()));
			count++;
		}
		assertEquals(
				1,
				count);

	}

	@Test
	public void testMax()
			throws IllegalArgumentException,
			NoSuchElementException,
			IOException {
		final GeoWaveFeatureReader reader = (GeoWaveFeatureReader) dataStore.getFeatureReader(
				type.getTypeName(),
				query);
		final MaxVisitor visitor = new MaxVisitor(
				"start",
				type);
		reader.getFeatureCollection().accepts(
				visitor,
				null);
		assertTrue(visitor.getMax().equals(
				etime));

	}

	@Test
	public void testMin()
			throws IllegalArgumentException,
			NoSuchElementException,
			IOException {
		final GeoWaveFeatureReader reader = (GeoWaveFeatureReader) dataStore.getFeatureReader(
				type.getTypeName(),
				query);
		final MinVisitor visitor = new MinVisitor(
				"start",
				type);
		reader.getFeatureCollection().accepts(
				visitor,
				null);
		assertTrue(visitor.getMin().equals(
				stime));

	}
}
