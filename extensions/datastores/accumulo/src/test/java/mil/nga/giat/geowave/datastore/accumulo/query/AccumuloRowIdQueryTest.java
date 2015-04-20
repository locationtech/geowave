package mil.nga.giat.geowave.datastore.accumulo.query;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloRowIdQueryTest
{
	private ByteArrayId rowId1;
	private ByteArrayId rowId2;
	private DataStore mockDataStore;
	private Index index;
	private WritableDataAdapter<AccumuloRangeQueryTest.TestGeometry> adapter;

	@Before
	public void ingestRow()
			throws AccumuloException,
			AccumuloSecurityException {
		final MockInstance mockInstance = new MockInstance();
		final Connector mockConnector = mockInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));
		mockDataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						mockConnector));

		adapter = new AccumuloRangeQueryTest().createGeometryAdapter();
		index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final GeometryFactory factory = new GeometryFactory();
		rowId1 = mockDataStore.ingest(
				adapter,
				index,
				new AccumuloRangeQueryTest.TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_1")).get(
				0);
		rowId2 = mockDataStore.ingest(
				adapter,
				index,
				new AccumuloRangeQueryTest.TestGeometry(
						factory.createPoint(new Coordinate(
								65,
								85)),
						"test_pt_2")).get(
				0);
	}

	@Test
	public void testGetRowById() {
		final AccumuloRangeQueryTest.TestGeometry geom1 = mockDataStore.getEntry(
				index,
				rowId1);
		Assert.assertEquals(
				"test_pt_1",
				geom1.id);
		final AccumuloRangeQueryTest.TestGeometry geom2 = mockDataStore.getEntry(
				index,
				rowId2);
		Assert.assertEquals(
				"test_pt_2",
				geom2.id);
	}
}
