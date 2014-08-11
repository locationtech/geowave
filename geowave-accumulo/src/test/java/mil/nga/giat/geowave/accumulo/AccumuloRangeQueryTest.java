package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
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
import mil.nga.giat.geowave.store.query.Query;
import mil.nga.giat.geowave.store.query.SpatialQuery;

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

public class AccumuloRangeQueryTest {
	private DataStore mockDataStore;
	private Index index;
	private WritableDataAdapter<TestGeometry> adapter;
	private final GeometryFactory factory = new GeometryFactory();
    private final TestGeometry testdata = new TestGeometry(factory.createPolygon(new Coordinate[] { new Coordinate(1.025, 1.032), new Coordinate(1.026, 1.032), new Coordinate(1.026, 1.033), new Coordinate(1.025, 1.032) }), "test_shape_1");
	@Before
	public void ingestGeometries()
			throws AccumuloException,
			AccumuloSecurityException {
		final MockInstance mockInstance = new MockInstance();
		final Connector mockConnector = mockInstance.getConnector("root", new PasswordToken(new byte[0]));
		mockDataStore = new AccumuloDataStore(new BasicAccumuloOperations(mockConnector));

		index = IndexType.SPATIAL.createDefaultIndex();
		adapter = new TestGeometryAdapter();
		
		mockDataStore.ingest(adapter, index, testdata).get(0);		
	}

	@Test
	public void testIntersection() {
		Geometry testGeo = factory.createPolygon(new Coordinate[] { new Coordinate(1.0249, 1.0319), new Coordinate(1.0261, 1.0319), new Coordinate(1.0261, 1.0323), new Coordinate(1.0249, 1.0319) });
		Query intersectQuery = new SpatialQuery(testGeo);
		Assert.assertTrue(testdata.geom.intersects(testGeo));
		CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(index, intersectQuery);
		Assert.assertTrue(resultOfIntersect.hasNext());
	}
	
	@Test
	public void testMiss() {
		Query intersectQuery = new SpatialQuery(factory.createPolygon(new Coordinate[] { new Coordinate(1.0247, 1.0319), new Coordinate(1.0249, 1.0319), new Coordinate(1.0249, 1.0323), new Coordinate(1.0247, 1.0319) }));
		CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(index, intersectQuery);
		Assert.assertFalse(resultOfIntersect.hasNext());
	}
	
	@Test
	public void testEncompass() {
		Query encompassQuery = new SpatialQuery(factory.createPolygon(new Coordinate[] { new Coordinate(1.0249, 1.0319), new Coordinate(1.0261, 1.0319), new Coordinate(1.0261, 1.0331), new Coordinate(1.0249, 1.0319) }));
		CloseableIterator<TestGeometry> resultOfIntersect = mockDataStore.query(index, encompassQuery);
		Assert.assertTrue(resultOfIntersect.hasNext());		
		final TestGeometry geom1 = resultOfIntersect.next();
		Assert.assertEquals("test_shape_1", geom1.id);
	}

	protected static class TestGeometry {
		protected final Geometry geom;
		protected final String id;

		public TestGeometry( final Geometry geom, final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected WritableDataAdapter<TestGeometry> createGeometryAdapter() {
		return new TestGeometryAdapter();
	}
	protected static class TestGeometryAdapter extends AbstractDataAdapter<TestGeometry> {
		private static final ByteArrayId GEOM = new ByteArrayId("myGeo");
		private static final ByteArrayId ID = new ByteArrayId("myId");
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {
			
			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] { GEOM };
			}

			@Override
			public CommonIndexValue toIndexValue( final TestGeometry row ) {
				return new GeometryWrapper(row.geom, new byte[0]);
			}

			@Override
			public PersistentValue<Object>[] toNativeValues( final CommonIndexValue indexValue ) {
				return new PersistentValue[] { new PersistentValue<Object>(GEOM, ((GeometryWrapper) indexValue).getGeometry()) };
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary( final byte[] bytes ) {

			}
		};
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<AccumuloRangeQueryTest.TestGeometry, Object>() {

			@Override
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue( final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestGeometry, Object>>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>>();
		static {
			COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
			NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		}

		public TestGeometryAdapter() {
			super(COMMON_FIELD_HANDLER_LIST, NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId("test");
		}

		@Override
		public boolean isSupported( final TestGeometry entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId( final TestGeometry entry ) {
			return new ByteArrayId(entry.id);
		}

		@Override
		public FieldReader getReader( final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return new GeometryReader();
			} else if (fieldId.equals(ID)) { return new StringReader(); }
			return null;
		}

		@Override
		public FieldWriter getWriter( final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return new GeometryWriter();
			} else if (fieldId.equals(ID)) { return new StringWriter(); }
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public void setField( final PersistentValue<Object> fieldValue ) {
					if (fieldValue.getId().equals(GEOM)) {
						geom = (Geometry) fieldValue.getValue();
					} else if (fieldValue.getId().equals(ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public TestGeometry buildRow( final ByteArrayId dataId ) {
					return new TestGeometry(geom, id);
				}
			};
		}

	}
}
