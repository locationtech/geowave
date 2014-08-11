package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.field.BasicReader.GeometryReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
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

		index = IndexType.SPATIAL.createDefaultIndex();
		adapter = new AccumuloRangeQueryTest().createGeometryAdapter();
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
