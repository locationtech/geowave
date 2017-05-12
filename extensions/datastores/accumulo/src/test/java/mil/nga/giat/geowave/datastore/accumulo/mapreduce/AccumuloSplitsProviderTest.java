package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldTypeStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;
//@formatter:off
/*if[accumulo.api=1.6]
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.data.KeyExtent;
else[accumulo.api=1.6]*/
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.data.impl.KeyExtent;
/*end[accumulo.api=1.6]*/
//@formatter:on
public class AccumuloSplitsProviderTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSplitsProviderTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();
	final GeometryFactory factory = new GeometryFactory();
	AccumuloOperations accumuloOperations;
	AccumuloIndexStore indexStore;
	AccumuloAdapterStore adapterStore;
	AccumuloDataStatisticsStore statsStore;
	AccumuloDataStore mockDataStore;
	AccumuloSecondaryIndexDataStore secondaryIndexDataStore;
	AdapterIndexMappingStore adapterIndexMappingStore;
	TabletLocator tabletLocator;
	PrimaryIndex index;
	WritableDataAdapter<TestGeometry> adapter;
	Geometry testGeoFilter;
	Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache;

	/**
	 * public List<InputSplit> getSplits(
	 * 
	 * final DistributableQuery query, final QueryOptions queryOptions,
	 * 
	 * final IndexStore indexStore,
	 * 
	 * final AdapterIndexMappingStore adapterIndexMappingStore, final Integer
	 * minSplits, final Integer maxSplits )
	 */

	@Before
	public void setUp() {
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
		accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		indexStore = new AccumuloIndexStore(
				accumuloOperations);

		adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);

		secondaryIndexDataStore = new AccumuloSecondaryIndexDataStore(
				accumuloOperations,
				new AccumuloOptions());

		adapterIndexMappingStore = new AccumuloAdapterIndexMappingStore(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				secondaryIndexDataStore,
				adapterIndexMappingStore,
				accumuloOperations,
				accumuloOptions);

		index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		adapter = new TestGeometryAdapter();

		tabletLocator = mock(TabletLocator.class);

		testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});

		statsCache = new HashMap<PrimaryIndex, RowRangeHistogramStatistics<?>>();
	}

	@Test
	public void testPopulateIntermediateSplits_EmptyRange() {
		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);
		final SplitsProvider splitsProvider = new MockAccumuloSplitsProvider(
				tabletLocator) {
			@Override
			public void addMocks() {
				doNothing().when(
						tabletLocator).invalidateCache();
			}
		};
		try {
			List<InputSplit> splits = splitsProvider.getSplits(
					accumuloOperations,
					query,
					new QueryOptions(
							adapter,
							index,
							-1,
							null,
							new String[] {
								"aaa",
								"bbb"
							}),
					adapterStore,
					statsStore,
					indexStore,
					adapterIndexMappingStore,
					1,
					5);
			verify(
					tabletLocator).invalidateCache();
			assertThat(
					splits.isEmpty(),
					is(true));
		}
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
			assertFalse(
					"Not expecting an error",
					true);
		}
	}

	/**
	 * Used to simulate what happens if an HBase operations for instance gets
	 * passed in
	 * 
	 * @author akash_000
	 *
	 */
	private static class MockOperations implements
			DataStoreOperations
	{
		public boolean tableExists(
				String altIdxTableName )
				throws IOException {
			return false;
		}

		public void deleteAll()
				throws Exception {}

		public String getTableNameSpace() {
			return null;
		}

		@Override
		public boolean mergeData(
				PrimaryIndex index,
				AdapterStore adapterStore,
				AdapterIndexMappingStore adapterIndexMappingStore ) {
			// TODO Auto-generated method stub
			return false;
		}
	}

	@Test
	public void testPopulateIntermediateSplits_MismatchedOperations() {
		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);
		final SplitsProvider splitsProvider = new MockAccumuloSplitsProvider(
				tabletLocator) {
			@Override
			public void addMocks() {
				// no mocks
			}
		};
		try {
			List<InputSplit> splits = splitsProvider.getSplits(
					new MockOperations(),
					query,
					new QueryOptions(
							adapter,
							index,
							-1,
							null,
							new String[] {
								"aaa",
								"bbb"
							}),
					adapterStore,
					statsStore,
					indexStore,
					adapterIndexMappingStore,
					1,
					5);
			assertThat(
					splits.isEmpty(),
					is(true));
			// no need to verify mock here, no actions taken on it
		}
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPopulateIntermediateSplits_UnsupportedQuery() {
		final SpatialQuery query = new SpatialQuery(
				testGeoFilter) {
			@Override
			public boolean isSupported(
					final Index<?, ?> index ) {
				return false;
			}
		};

		final SplitsProvider splitsProvider = new MockAccumuloSplitsProvider(
				tabletLocator) {
			@Override
			public void addMocks() {
				// no mocks
			}
		};
		try {
			List<InputSplit> splits = splitsProvider.getSplits(
					accumuloOperations,
					query,
					new QueryOptions(
							adapter,
							index,
							-1,
							null,
							new String[] {
								"aaa",
								"bbb"
							}),
					adapterStore,
					statsStore,
					indexStore,
					adapterIndexMappingStore,
					1,
					5);
			// if query is unsupported, return an empty split, with no error
			assertThat(
					splits.isEmpty(),
					is(true));
			// no need to verify mock here, no actions taken on it
		}
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPopulateIntermediateSplits_PrebuiltRange() {
		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);

		final SplitsProvider splitsProvider = new MockAccumuloSplitsProvider(
				tabletLocator,
				new AccumuloRowRange(
						new Range(
								"aa",
								"bb"))) {
			@Override
			public void addMocks() {
				doNothing().when(
						tabletLocator).invalidateCache();
				try {
					when(
							tabletLocator.binRanges(
									isA(
//@formatter:off
									/*if[accumulo.api=1.6]
 									Credentials.class
						  			else[accumulo.api=1.6]*/
									ClientContext.class
							  		/*end[accumulo.api=1.6]*/								
//@formatter:on
									),
									anyList(),
									anyMap())).thenReturn(
							new ArrayList<Range>());
				}
				catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
					e.printStackTrace();
				}
			}

			@Override
			/**
			 * Build our own simple Binned Range Structure
			 * Here is what it will look like:
			 * 
			 * Initial level: Map of String (tablet locations) to another Map
			 * Second level: Map of KeyExtents (the difference between our previous range and current range) to Range lists
			 * Third level: List of the range lists
			 * 
			 * This implementation will have two tablet locations (assumed to be two nodes), 4 KeyExtents per tablet location (3, 3, 3, 4) with the total range spanning a to z
			 * @return
			 */
			public Map<String, Map<KeyExtent, List<Range>>> getBinnedRangesStructure() {

				final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges = new HashMap<String, Map<KeyExtent, List<Range>>>();

				// first tablet locator
				tserverBinnedRanges.put(
						"127.0.0.1:tabletLocator1",
						new HashMap<KeyExtent, List<Range>>());
				// second tablet locator
				tserverBinnedRanges.put(
						"127.0.0.2:tabletLocator2",
						new HashMap<KeyExtent, List<Range>>());

				final Text table = new Text(
						"GEOWAVE_METADATA");
				// key extents for first tablet locator
				tserverBinnedRanges.get(
						"127.0.0.1:tabletLocator1").put(
						new KeyExtent(
								table,
								new Text(
										"ac"),
								new Text(
										"")),
						Arrays.asList(
								new Range(
										"aa",
										"ab"),
								new Range(
										"ab",
										"ac")));
				tserverBinnedRanges.get(
						"127.0.0.1:tabletLocator1").put(
						new KeyExtent(
								table,
								new Text(
										"af"),
								new Text(
										"ac")),
						Arrays.asList(
								new Range(
										"ac",
										"ad"),
								new Range(
										"ad",
										"ae"),
								new Range(
										"ae",
										"af")));
				tserverBinnedRanges.get(
						"127.0.0.1:tabletLocator1").put(
						new KeyExtent(
								table,
								new Text(
										"ai"),
								new Text(
										"af")),
						Arrays.asList(
								new Range(
										"af",
										"ag"),
								new Range(
										"ag",
										"ah"),
								new Range(
										"ah",
										"ai")));
				tserverBinnedRanges.get(
						"127.0.0.1:tabletLocator1").put(
						new KeyExtent(
								table,
								new Text(
										"am"),
								new Text(
										"ai")),
						Arrays.asList(
								new Range(
										"ai",
										"aj"),
								new Range(
										"aj",
										"ak"),
								new Range(
										"ak",
										"al"),
								new Range(
										"al",
										"am")));

				// key extents for second tablet locator
				tserverBinnedRanges.get(
						"127.0.0.2:tabletLocator2").put(
						new KeyExtent(
								table,
								new Text(
										"ao"),
								new Text(
										"am")),
						Arrays.asList(
								new Range(
										"am",
										"an"),
								new Range(
										"an",
										"ao")));
				tserverBinnedRanges.get(
						"127.0.0.2:tabletLocator2").put(
						new KeyExtent(
								table,
								new Text(
										"ar"),
								new Text(
										"ao")),
						Arrays.asList(
								new Range(
										"ao",
										"ap"),
								new Range(
										"ap",
										"aq"),
								new Range(
										"aq",
										"ar")));
				tserverBinnedRanges.get(
						"127.0.0.2:tabletLocator2").put(
						new KeyExtent(
								table,
								new Text(
										"au"),
								new Text(
										"ar")),
						Arrays.asList(
								new Range(
										"ar",
										"as"),
								new Range(
										"as",
										"at"),
								new Range(
										"at",
										"au")));
				tserverBinnedRanges.get(
						"127.0.0.2:tabletLocator2").put(
						new KeyExtent(
								table,
								new Text(
										"ay"),
								new Text(
										"au")),
						Arrays.asList(
								new Range(
										"au",
										"av"),
								new Range(
										"av",
										"aw"),
								new Range(
										"aw",
										"ax"),
								new Range(
										"ax",
										"ay")));
				tserverBinnedRanges.get(
						"127.0.0.2:tabletLocator2").put(
						new KeyExtent(
								table,
								new Text(
										"bb"),
								new Text(
										"ay")),
						Arrays.asList(
								new Range(
										"ay",
										"az"),
								new Range(
										"az",
										"ba"),
								new Range(
										"ba",
										"bb")));

				return tserverBinnedRanges;
			}

			/**
			 * Build our own host name cache, to avoid an unsuccessful lookup
			 * Expect two tablets, just use default localhost ip
			 */
			@Override
			public HashMap<String, String> getHostNameCache() {
				final HashMap<String, String> hostNameCache = new HashMap<String, String>();
				hostNameCache.put(
						"127.0.0.1",
						"tabletLocator1");
				hostNameCache.put(
						"127.0.0.2",
						"tabletLocator2");
				return hostNameCache;
			}
		};
		try {
			QueryOptions queryOptions = new QueryOptions(
					adapter,
					index,
					-1,
					null,
					new String[] {
						"aaa",
						"bbb"
					});

			Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair = queryOptions
					.getAdaptersWithMinimalSetOfIndices(
							adapterStore,
							adapterIndexMappingStore,
							indexStore)
					.get(
							0);

			TreeSet<IntermediateSplitInfo> splitsInput = new TreeSet<IntermediateSplitInfo>();
			TreeSet<IntermediateSplitInfo> splitsOutput;

			splitsOutput = ((MockAccumuloSplitsProvider) splitsProvider).populateIntermediateSplits(
					splitsInput,
					accumuloOperations,
					indexAdapterPair.getLeft(),
					indexAdapterPair.getValue(),
					statsCache,
					adapterStore,
					statsStore,
					5,
					query,
					queryOptions.getAuthorizations());

			verify(
					tabletLocator).invalidateCache();
			verify(
					tabletLocator).binRanges(
					isA(
//@formatter:off
					/*if[accumulo.api=1.6]
					Credentials.class
		  			else[accumulo.api=1.6]*/
					ClientContext.class
			  		/*end[accumulo.api=1.6]*/								
//@formatter:on
					),
					anyList(),
					anyMap());

			// if query is unsupported, return an empty split, with no error
			assertThat(
					splitsOutput.isEmpty(),
					is(false));
			assertThat(
					splitsOutput.size(),
					is(9));
			IntermediateSplitInfo splitTest;
			int countTablet1 = 0;
			int countTablet2 = 0;

			splitTest = splitsOutput.pollFirst();
			// can't verify order of splits; I was getting different order
			// depending on whether I ran or debugged
			// instead, verify size of splits
			while (splitTest != null) {
				GeoWaveInputSplit finalSplitTest = splitTest.toFinalSplit();
				if (finalSplitTest.getLocations()[0] == "tabletLocator1") {
					countTablet1++;
				}
				else if (finalSplitTest.getLocations()[0] == "tabletLocator2") {
					countTablet2++;
				}
				splitTest = splitsOutput.pollFirst();
			}

			assertThat(
					countTablet1,
					is(4));
			assertThat(
					countTablet2,
					is(5));
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (AccumuloException e) {
			e.printStackTrace();
		}
		catch (AccumuloSecurityException e) {
			e.printStackTrace();
		}
		catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testWrapRange() {
		final ByteBuffer startRow = ByteBuffer.allocate(8);
		startRow.put("aaaaaaaa".getBytes());
		startRow.rewind();
		final ByteBuffer endRow = ByteBuffer.allocate(8);
		endRow.put("bbbbbbbb".getBytes());
		endRow.rewind();
		final ByteBuffer colFamily = ByteBuffer.allocate(8);
		colFamily.put("testing".getBytes());
		colFamily.rewind();
		final ByteBuffer colQualifier = ByteBuffer.allocate(8);
		colQualifier.put("testing".getBytes());
		colQualifier.rewind();
		final ByteBuffer colVisibility = ByteBuffer.allocate(8);
		colVisibility.put("testing".getBytes());
		colVisibility.rewind();
		TRange tRange = new TRange(
				new TKey(
						startRow,
						colFamily,
						colQualifier,
						colVisibility,
						new Date().getTime()),
				new TKey(
						endRow,
						colFamily,
						colQualifier,
						colVisibility,
						new Date().getTime()),
				true,
				false,
				false,
				false);

		Map<String, Range> rangesToTest = new HashMap<String, Range>();
		rangesToTest.put(
				"emptyRange",
				new Range());

		rangesToTest.put(
				"charSequenceRange1",
				new Range(
						"The quick fox jumps over the lazy dog"));
		rangesToTest.put(
				"textRange",
				new Range(
						new Text(
								"The quick fox jumps over the lazy dog")));
		rangesToTest.put(
				"thriftRange",
				new Range(
						tRange));
		rangesToTest.put(
				"charSequenceRange2",
				new Range(
						"The quick fox jumps over the lazy dog",
						"quick fox jumps over the lazy dog"));
		rangesToTest.put(
				"keyRange",
				new Range(
						new Key(
								new Text(
										"alpha")),
						new Key(
								new Text(
										"epsilon"))));

		for (String key : rangesToTest.keySet()) {
			GeoWaveRowRange wrappedRange = AccumuloSplitsProvider.wrapRange(rangesToTest.get(key));

			String wrappedStartKey = wrappedRange.getStartKey() != null ? new String(
					wrappedRange.getStartKey()) : null;
			String originalStartKey = rangesToTest.get(
					key).getStartKey() != null ? rangesToTest.get(
					key).getStartKey().getRow().toString() : null;
			assertThat(
					"StartKey test case failed: " + key,
					wrappedStartKey,
					is(originalStartKey));

			String wrappedEndKey = wrappedRange.getEndKey() != null ? new String(
					wrappedRange.getEndKey()) : null;
			String originalEndKey = rangesToTest.get(
					key).getEndKey() != null ? rangesToTest.get(
					key).getEndKey().getRow().toString() : null;
			assertThat(
					"StartKey test case failed: " + key,
					wrappedEndKey,
					is(originalEndKey));
		}
	}

	@Test
	public void testUnwrapRanges() {
		final ByteBuffer startRow = ByteBuffer.allocate(8);
		startRow.put("aaaaaaaa".getBytes());
		startRow.rewind();
		final ByteBuffer endRow = ByteBuffer.allocate(8);
		endRow.put("bbbbbbbb".getBytes());
		endRow.rewind();
		final ByteBuffer colFamily = ByteBuffer.allocate(8);
		colFamily.put("testing".getBytes());
		colFamily.rewind();
		final ByteBuffer colQualifier = ByteBuffer.allocate(8);
		colQualifier.put("testing".getBytes());
		colQualifier.rewind();
		final ByteBuffer colVisibility = ByteBuffer.allocate(8);
		colVisibility.put("testing".getBytes());
		colVisibility.rewind();
		TRange tRange = new TRange(
				new TKey(
						startRow,
						colFamily,
						colQualifier,
						colVisibility,
						new Date().getTime()),
				new TKey(
						endRow,
						colFamily,
						colQualifier,
						colVisibility,
						new Date().getTime()),
				true,
				false,
				false,
				false);

		Map<String, Range> rangesToTest = new HashMap<String, Range>();
		rangesToTest.put(
				"emptyRange",
				new Range());

		rangesToTest.put(
				"charSequenceRange1",
				new Range(
						"The quick fox jumps over the lazy dog"));
		rangesToTest.put(
				"textRange",
				new Range(
						new Text(
								"The quick fox jumps over the lazy dog")));
		rangesToTest.put(
				"thriftRange",
				new Range(
						tRange));
		rangesToTest.put(
				"charSequenceRange2",
				new Range(
						"The quick fox jumps over the lazy dog",
						"quick fox jumps over the lazy dog"));
		rangesToTest.put(
				"keyRange",
				new Range(
						new Key(
								new Text(
										"alpha")),
						new Key(
								new Text(
										"epsilon"))));

		// easiest way to test unwrap is by verifying an unwrapped wrap is equal
		// to itself
		for (String key : rangesToTest.keySet()) {
			assertThat(
					rangesToTest.get(key),
					is(AccumuloSplitsProvider.unwrapRange(AccumuloSplitsProvider.wrapRange(rangesToTest.get(key)))));
		}
	}

	protected static class TestGeometry
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

	protected static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry> implements
			StatisticsProvider<TestGeometry>
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

			@SuppressWarnings("unchecked")
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

		private final static EntryVisibilityHandler<TestGeometry> GEOMETRY_VISIBILITY_HANDLER = new FieldTypeStatisticVisibility<TestGeometry>(
				GeometryWrapper.class);
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

		@SuppressWarnings("unchecked")
		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final ByteArrayId fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		public DataStatistics<TestGeometry> createDataStatistics(
				final ByteArrayId statisticsId ) {
			if (BoundingBoxDataStatistics.STATS_TYPE.equals(statisticsId)) {
				return new GeoBoundingBoxStatistics(
						getAdapterId());
			}
			else if (CountDataStatistics.STATS_TYPE.equals(statisticsId)) {
				return new CountDataStatistics<TestGeometry>(
						getAdapterId());
			}
			LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + " using count statistic");
			return new CountDataStatistics<TestGeometry>(
					getAdapterId(),
					statisticsId);
		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				final ByteArrayId statisticsId ) {
			return GEOMETRY_VISIBILITY_HANDLER;
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

		@Override
		public ByteArrayId[] getSupportedStatisticsTypes() {
			return SUPPORTED_STATS_IDS;
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final ByteArrayId fieldId ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldId())) {
					return i;
				}
				i++;
			}
			if (fieldId.equals(GEOM)) {
				return i;
			}
			else if (fieldId.equals(ID)) {
				return i + 1;
			}
			return -1;
		}

		@Override
		public ByteArrayId getFieldIdForPosition(
				final CommonIndexModel model,
				final int position ) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldId();
					}
					i++;
				}
			}
			else {
				final int numDimensions = model.getDimensions().length;
				if (position == numDimensions) {
					return GEOM;
				}
				else if (position == (numDimensions + 1)) {
					return ID;
				}
			}
			return null;
		}
	}

	private final static ByteArrayId[] SUPPORTED_STATS_IDS = new ByteArrayId[] {
		BoundingBoxDataStatistics.STATS_TYPE,
		CountDataStatistics.STATS_TYPE
	};

	private static class GeoBoundingBoxStatistics extends
			BoundingBoxDataStatistics<TestGeometry>
	{

		@SuppressWarnings("unused")
		protected GeoBoundingBoxStatistics() {
			super();
		}

		public GeoBoundingBoxStatistics(
				final ByteArrayId dataAdapterId ) {
			super(
					dataAdapterId);
		}

		@Override
		protected Envelope getEnvelope(
				final TestGeometry entry ) {
			// incorporate the bounding box of the entry's envelope
			final Geometry geometry = entry.geom;
			if ((geometry != null) && !geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
			return null;
		}

	}

	private abstract static class MockAccumuloSplitsProvider extends
			AccumuloSplitsProvider
	{
		private GeoWaveRowRange rangeMax;
		private TabletLocator mockTabletLocator;

		public MockAccumuloSplitsProvider(
				TabletLocator tabletLocator ) {
			super();
			this.mockTabletLocator = tabletLocator;
		}

		public MockAccumuloSplitsProvider(
				TabletLocator tabletLocator,
				GeoWaveRowRange rangeMax ) {
			super();
			this.rangeMax = rangeMax;
			this.mockTabletLocator = tabletLocator;
		}

		public abstract void addMocks();

		/**
		 * Return a mocked out TabletLocator to avoid having to look up a
		 * TabletLocator, which fails
		 *
		 */
		@Override
		protected TabletLocator getTabletLocator(
				final Object clientContextOrInstance,
				final String tableId )
				throws TableNotFoundException {
			addMocks();
			return mockTabletLocator;
		}

		/**
		 * Returns a rangeMax, instead of going through the statistics store to
		 * get it
		 */
		@Override
		protected GeoWaveRowRange getRangeMax(
				final Index<?, ?> index,
				final AdapterStore adapterStore,
				final DataStatisticsStore statsStore,
				final String[] authorizations ) {
			return this.rangeMax;
		};

		@Override
		public TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
				final TreeSet<IntermediateSplitInfo> splits,
				final DataStoreOperations operations,
				final PrimaryIndex index,
				final List<DataAdapter<Object>> adapters,
				final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
				final AdapterStore adapterStore,
				final DataStatisticsStore statsStore,
				final Integer maxSplits,
				final DistributableQuery query,
				final String[] authorizations )
				throws IOException {
			return super.populateIntermediateSplits(
					splits,
					operations,
					index,
					adapters,
					statsCache,
					adapterStore,
					statsStore,
					maxSplits,
					query,
					authorizations);
		}
	}
}