package mil.nga.giat.geowave.core.store.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.IntegerRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.TestIndexModel;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.junit.Test;

public class MemoryDataStoreTest
{

	@Test
	public void test()
			throws IOException {
		final PrimaryIndex index = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel());
		final String namespace = "test_" + getClass().getName();
		final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				new HashMap<String, Object>(),
				namespace);
		final DataStatisticsStore statsStore = storeFamily.getDataStatisticsStoreFactory().createStore(
				new HashMap<String, Object>(),
				namespace);
		final WritableDataAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

		try (final IndexWriter indexWriter = dataStore.createIndexWriter(
				index,
				new VisibilityWriter<Integer>() {
					@Override
					public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
							ByteArrayId fieldId ) {
						return new GlobalVisibilityHandler(
								"aaa&bbb");
					}
				})) {

			indexWriter.write(
					adapter,
					new Integer(
							25));
			indexWriter.flush();

			indexWriter.write(
					adapter,
					new Integer(
							35));
			indexWriter.flush();
		}

		// authorization check
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}

		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}

		final Iterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics();
		assertTrue(checkStats(
				(DataStatistics<Integer>) statsIt.next(),
				2,
				new NumericRange(
						25,
						35)));
		assertTrue(checkStats(
				(DataStatistics<Integer>) statsIt.next(),
				2,
				new NumericRange(
						25,
						35)));

		dataStore.delete(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26));
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new DataIdQuery(
						adapter.getAdapterId(),
						adapter.getDataId(new Integer(
								35))))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}

	}

	private boolean checkStats(
			final DataStatistics<Integer> stat,
			final int count,
			final NumericRange range ) {
		if (stat instanceof CountDataStatistics) {
			return ((CountDataStatistics) stat).getCount() == count;
		}
		else if (stat instanceof IntegerRangeDataStatistics) {
			return (((IntegerRangeDataStatistics) stat).getMin() == range.getMin()) && (((IntegerRangeDataStatistics) stat).getMax() == range.getMax());
		}
		else if (stat instanceof RowRangeDataStatistics) {
			return true;
		}
		else if (stat instanceof RowRangeHistogramStatistics) {
			return true;
		}
		return false;
	}

	private class TestQueryFilter implements
			QueryFilter
	{
		final CommonIndexModel indexModel;
		final double min, max;

		public TestQueryFilter(
				final CommonIndexModel indexModel,
				final double min,
				final double max ) {
			super();
			this.indexModel = indexModel;
			this.min = min;
			this.max = max;
		}

		@Override
		public boolean accept(
				final CommonIndexModel indexModel,
				final IndexedPersistenceEncoding<?> persistenceEncoding ) {
			final double min = ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
					indexModel.getDimensions()).getDataPerDimension()[0].getMin();
			final double max = ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
					indexModel.getDimensions()).getDataPerDimension()[0].getMax();
			return !((this.max <= min) || (this.min > max));
		}

	}

	private class TestQuery implements
			Query
	{

		final double min, max;

		public TestQuery(
				final double min,
				final double max ) {
			super();
			this.min = min;
			this.max = max;
		}

		@Override
		public List<QueryFilter> createFilters(
				final CommonIndexModel indexModel ) {
			return Arrays.asList((QueryFilter) new TestQueryFilter(
					indexModel,
					min,
					max));
		}

		@Override
		public boolean isSupported(
				final Index<?, ?> index ) {
			return ((PrimaryIndex) index).getIndexModel() instanceof TestIndexModel;
		}

		@Override
		public List<MultiDimensionalNumericData> getIndexConstraints(
				final NumericIndexStrategy indexStrategy ) {
			return Collections.<MultiDimensionalNumericData> singletonList(new BasicNumericDataset(
					new NumericData[] {
						new NumericRange(
								min,
								max)
					}));
		}

	}
}
