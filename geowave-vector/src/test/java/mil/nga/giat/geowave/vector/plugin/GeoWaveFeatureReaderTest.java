package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
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
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));
	Query query = null;
	List<String> fids = new ArrayList<String>();

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			Exception {
		dataStore = new GeoWaveGTMemDataStore();
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		dataStore.createSchema(type);

		final Transaction transaction1 = new DefaultTransaction();
		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertFalse(writer.hasNext());
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(
						100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
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
				Long.valueOf(
						101));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
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
				"geostuff",
				ECQL.toFilter("IN ('" + fids.get(0) + "')"),
				new String[] {
					"geometry",
					"pid"
				});
	}

	@Test
	public void test()
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

}
