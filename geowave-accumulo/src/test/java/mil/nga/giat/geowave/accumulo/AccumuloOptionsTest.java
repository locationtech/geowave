package mil.nga.giat.geowave.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloOptionsTest
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloOptionsTest.class);

	@Test
	public void testIndexOptions() {

		// setup accumulo instance
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}

		final AccumuloOptions accumuloOptions = new AccumuloOptions();

		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		final DataStore mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				accumuloOperations,
				accumuloOptions);

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final GeometryFactory factory = new GeometryFactory();

		accumuloOptions.setCreateIndex(false);
		accumuloOptions.setPersistIndex(false);

		final List<ByteArrayId> rowIds = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt"));

		// as the table didn't already exist, the flag indicates not to create
		// it, so no rows will be returned
		Assert.assertEquals(
				0,
				rowIds.size());

		accumuloOptions.setCreateIndex(true);

		final ByteArrayId rowId1 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_1")).get(
				0);

		// as we have chosen not to persist the index, we will not see an index
		// entry in the index store
		Assert.assertEquals(
				false,
				indexStore.indexExists(index.getId()));

		final TestGeometry geom1 = mockDataStore.getEntry(
				index,
				rowId1);

		// even though we didn't persist the index, the test point was still
		// stored
		Assert.assertEquals(
				"test_pt_1",
				geom1.id);

		accumuloOptions.setPersistIndex(true);

		final ByteArrayId rowId2 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_2")).get(
				0);

		// as we have chosen to persist the index, we will see the index entry
		// in the index store
		Assert.assertEquals(
				true,
				indexStore.indexExists(index.getId()));

		final TestGeometry geom2 = mockDataStore.getEntry(
				index,
				rowId2);

		// of course, the point is actually stored in this case
		Assert.assertEquals(
				"test_pt_2",
				geom2.id);
	}

	@Test
	public void testLocalityGroups() {

		// setup accumulo instance
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}

		final AccumuloOptions accumuloOptions = new AccumuloOptions();

		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		final DataStore mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				accumuloOperations,
				accumuloOptions);

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final GeometryFactory factory = new GeometryFactory();

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		final byte[] adapterId = adapter.getAdapterId().getBytes();

		accumuloOptions.setUseLocalityGroups(false);

		final ByteArrayId rowId1 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_1")).get(
				0);

		try {
			// as we are not using locality groups, we expect that this will
			// return false
			Assert.assertEquals(
					false,
					accumuloOperations.localityGroupExists(
							tableName,
							adapterId));
		}
		catch (final AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Locality Group check failed",
					e);
		}

		final TestGeometry geom1 = mockDataStore.getEntry(
				index,
				rowId1);

		// of course, the point is actually stored in this case
		Assert.assertEquals(
				"test_pt_1",
				geom1.id);

		accumuloOptions.setUseLocalityGroups(true);

		final ByteArrayId rowId2 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_2")).get(
				0);

		try {
			// now that locality groups are turned on, we expect this to return
			// true
			Assert.assertEquals(
					true,
					accumuloOperations.localityGroupExists(
							tableName,
							adapterId));
		}
		catch (final AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Locality Group check failed",
					e);
		}

		final TestGeometry geom2 = mockDataStore.getEntry(
				index,
				rowId2);

		// of course, the point is actually stored in this case
		Assert.assertEquals(
				"test_pt_2",
				geom2.id);
	}

	@Test
	public void testAdapterOptions() {

		// setup accumulo instance
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}

		final AccumuloOptions accumuloOptions = new AccumuloOptions();

		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		final DataStore mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				accumuloOperations,
				accumuloOptions);

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final GeometryFactory factory = new GeometryFactory();

		accumuloOptions.setPersistAdapter(false);

		final ByteArrayId rowId1 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_1")).get(
				0);

		TestGeometry geom1 = mockDataStore.getEntry(
				index,
				rowId1);

		// without specifying the adapter, this method returns null
		Assert.assertEquals(
				null,
				geom1);

		final CloseableIterator<TestGeometry> geomItr = mockDataStore.query(
				adapter,
				null);

		geom1 = geomItr.next();

		try {
			geomItr.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Iterator close error",
					e);
		}

		// specifying the adapter, this method returns the entry
		Assert.assertEquals(
				"test_pt_1",
				geom1.id);

		// the adapter should not exist in the metadata table
		Assert.assertEquals(
				false,
				adapterStore.adapterExists(adapter.getAdapterId()));

		accumuloOptions.setPersistAdapter(true);

		final ByteArrayId rowId2 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_2")).get(
				0);

		final TestGeometry geom2 = mockDataStore.getEntry(
				index,
				rowId2);

		// specifying the adapter, this method returns the entry
		Assert.assertEquals(
				"test_pt_2",
				geom2.id);

		// the adapter should exist in the metadata table
		Assert.assertEquals(
				true,
				adapterStore.adapterExists(adapter.getAdapterId()));
	}

	@Test
	public void testAlternateIndexOption() {

		// setup accumulo instance
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}

		final AccumuloOptions accumuloOptions = new AccumuloOptions();

		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		final AccumuloDataStore mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				accumuloOperations,
				accumuloOptions);

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final GeometryFactory factory = new GeometryFactory();

		final ByteArrayId adapterId = adapter.getAdapterId();

		accumuloOptions.setUseAltIndex(false);

		final ByteArrayId rowId0 = mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_0")).get(
				0);

		TestGeometry geom0 = mockDataStore.getEntry(
				index,
				new ByteArrayId(
						"test_pt_0"),
				adapterId);

		// this should return our data correctly
		Assert.assertEquals(
				"test_pt_0",
				geom0.id);

		// delete entry by data id & adapter id
		mockDataStore.deleteEntry(
				index,
				new ByteArrayId(
						"test_pt_0"),
				adapterId);

		geom0 = mockDataStore.getEntry(
				index,
				new ByteArrayId(
						"test_pt_0"),
				adapterId);

		// this should return null as the entry was deleted
		Assert.assertEquals(
				null,
				geom0);

		accumuloOptions.setUseAltIndex(true);

		accumuloOperations.deleteAll();

		mockDataStore.ingest(
				adapter,
				index,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_1")).get(
				0);

		final TestGeometry geom1 = mockDataStore.getEntry(
				index,
				new ByteArrayId(
						"test_pt_1"),
				adapterId);

		// this should return our data correctly
		Assert.assertEquals(
				"test_pt_1",
				geom1.id);

		final ArrayList<TestGeometry> geomList = new ArrayList<TestGeometry>();
		geomList.add(new TestGeometry(
				factory.createPoint(new Coordinate(
						25,
						32)),
				"test_pt_2"));

		mockDataStore.ingest(
				adapter,
				index,
				geomList.iterator());

		final TestGeometry geom2 = mockDataStore.getEntry(
				index,
				new ByteArrayId(
						"test_pt_2"),
				adapterId);

		// this should return our data correctly
		Assert.assertEquals(
				"test_pt_2",
				geom2.id);

		final AccumuloIndexWriter indexWriter = new AccumuloIndexWriter(
				index,
				accumuloOperations,
				accumuloOptions,
				mockDataStore);

		indexWriter.write(
				adapter,
				new TestGeometry(
						factory.createPoint(new Coordinate(
								25,
								32)),
						"test_pt_3"));

		indexWriter.close();

		final TestGeometry geom3 = mockDataStore.getEntry(
				index,
				new ByteArrayId(
						"test_pt_3"),
				adapterId);

		// this should return our data correctly
		Assert.assertEquals(
				"test_pt_3",
				geom3.id);
	}

	private static class TestGeometry
	{
		private final Geometry geom;
		private final String id;

		public TestGeometry(
				final Geometry geom,
				final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	private static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry>
	{
		private static final ByteArrayId GEOM = new ByteArrayId(
				"myGeo");
		private static final ByteArrayId ID = new ByteArrayId(
				"myId");
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] {
					GEOM
				};
			}

			@Override
			public CommonIndexValue toIndexValue(
					final TestGeometry row ) {
				return new GeometryWrapper(
						row.geom,
						new byte[0]);
			}

			@Override
			public PersistentValue<Object>[] toNativeValues(
					final CommonIndexValue indexValue ) {
				return new PersistentValue[] {
					new PersistentValue<Object>(
							GEOM,
							((GeometryWrapper) indexValue).getGeometry())
				};
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary(
					final byte[] bytes ) {

			}
		};
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestGeometry, Object>() {

			@Override
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
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
			super(
					COMMON_FIELD_HANDLER_LIST,
					NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"test");
		}

		@Override
		public boolean isSupported(
				final TestGeometry entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final TestGeometry entry ) {
			return new ByteArrayId(
					entry.id);
		}

		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return new GeometryReader();
			}
			else if (fieldId.equals(ID)) {
				return new StringReader();
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return new GeometryWriter();
			}
			else if (fieldId.equals(ID)) {
				return new StringWriter();
			}
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public void setField(
						final PersistentValue<Object> fieldValue ) {
					if (fieldValue.getId().equals(
							GEOM)) {
						geom = (Geometry) fieldValue.getValue();
					}
					else if (fieldValue.getId().equals(
							ID)) {
						id = (String) fieldValue.getValue();
					}
				}

				@Override
				public TestGeometry buildRow(
						final ByteArrayId dataId ) {
					return new TestGeometry(
							geom,
							id);
				}
			};
		}
	}
}
