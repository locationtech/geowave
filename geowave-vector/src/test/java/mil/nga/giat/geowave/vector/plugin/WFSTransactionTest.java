package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class WFSTransactionTest
{
	GeoWaveGTMemDataStore dataStore;
	SimpleFeatureType schema;
	SimpleFeatureType type;
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));
	Query query = null;

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException {
		dataStore = new GeoWaveGTMemDataStore();
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		dataStore.createSchema(type);
		query = new Query(
				"geostuff",
				CQL.toFilter("BBOX(geometry,27.30,41.20,27.20,41.30)"),
				new String[] {
					"geometry",
					"pid"
				});
	}

	@Test
	public void testInsertIsolation()
			throws IOException,
			CQLException {
		final Transaction transaction1 = new DefaultTransaction();

		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertFalse(writer.hasNext());
		final SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		writer.write();
		writer.close();

		FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(
				query,
				transaction1);
		assertTrue(reader.hasNext());
		final SimpleFeature priorFeature = reader.next();
		assertEquals(
				newFeature.getAttribute("pid"),
				priorFeature.getAttribute("pid"));
		reader.close();

		// uncommitted at this point, so this next transaction should not see
		// it.

		final Transaction transaction2 = new DefaultTransaction();
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertFalse(reader.hasNext());
		reader.close();

		transaction1.commit();
		reader = dataStore.getFeatureReader(
				query,
				transaction1);
		assertTrue(reader.hasNext());
		reader.next();
		assertFalse(reader.hasNext());
		reader.close();

		transaction1.close();

		// since this implementation does not support serializable, transaction2
		// can see the changes even though
		// it started after transaction1 and before the commit.
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		reader.next();
		assertFalse(reader.hasNext());
		reader.close();
		transaction2.commit();
		transaction2.close();

	}

	// ==============
	// DELETION TEST
	@Test
	public void testDelete()
			throws IOException {

		Transaction transaction1 = new DefaultTransaction();

		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
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
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		writer.write();
		writer.close();
		transaction1.commit();
		transaction1.close();

		Transaction transaction2 = new DefaultTransaction();
		FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		SimpleFeature priorFeature = reader.next();
		reader.close();
		transaction2.commit();
		transaction2.close();

		// Add one more in this transaction and remove the
		// prior feature.

		final String idToRemove = priorFeature.getID();
		transaction1 = new DefaultTransaction();
		writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		while (writer.hasNext()) {
			writer.next();
		}
		newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				new Long(
						200));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		writer.write();
		writer.close();

		// Find the the prior one to remove
		writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertTrue(writer.hasNext());
		do {
			priorFeature = writer.next();
		}
		while (!priorFeature.getID().equals(
				idToRemove) && writer.hasNext());

		// make sure it is found
		assertTrue(priorFeature.getID().equals(
				idToRemove));
		writer.remove();
		writer.close();

		// make sure a new transaction can see (not committed)
		transaction2 = new DefaultTransaction();
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertFalse(reader.hasNext());
		assertTrue(priorFeature.getID().equals(
				idToRemove));
		reader.close();
		transaction2.commit();
		transaction2.close();

		// make sure existing transaction cannot see (not committed)
		reader = dataStore.getFeatureReader(
				query,
				transaction1);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertFalse(reader.hasNext());
		assertTrue(!priorFeature.getID().equals(
				idToRemove));
		reader.close();
		transaction1.commit();
		transaction1.close();

		// make sure a new transaction can not see (committed)
		transaction2 = new DefaultTransaction();
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertFalse(reader.hasNext());
		assertTrue(!priorFeature.getID().equals(
				idToRemove));
		reader.close();
		transaction2.commit();
		transaction2.close();
	}

	@Test
	public void testUpdate()
			throws IOException {
		Transaction transaction1 = new DefaultTransaction();

		FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertFalse(writer.hasNext());
		final SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		writer.write();
		writer.close();
		transaction1.commit();
		transaction1.close();

		// change the pid
		transaction1 = new DefaultTransaction();
		writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertTrue(writer.hasNext());
		SimpleFeature priorFeature = writer.next();
		final String pid = UUID.randomUUID().toString();
		priorFeature.setAttribute(
				"pid",
				pid);
		writer.write();
		writer.close();

		// check update
		FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(
				query,
				transaction1);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertEquals(
				pid,
				priorFeature.getAttribute("pid"));
		reader.close();

		// check isolation
		Transaction transaction2 = new DefaultTransaction();
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertFalse(reader.hasNext());
		assertTrue(!priorFeature.getAttribute(
				"pid").equals(
				pid));
		reader.close();
		transaction2.commit();
		transaction2.close();

		// commit change
		transaction1.commit();
		transaction1.close();

		// verify change
		transaction2 = new DefaultTransaction();
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		assertTrue(reader.hasNext());
		priorFeature = reader.next();
		assertFalse(reader.hasNext());
		assertTrue(priorFeature.getAttribute(
				"pid").equals(
				pid));
		reader.close();
		transaction2.commit();
		transaction2.close();
	}
}
