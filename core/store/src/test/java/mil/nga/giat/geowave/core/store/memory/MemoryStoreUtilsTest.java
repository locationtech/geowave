package mil.nga.giat.geowave.core.store.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryEntryRow;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreUtils;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStore;

import org.junit.Test;

public class MemoryStoreUtilsTest
{
	@Test
	public void testEntryToRows() {
		final AtomicInteger count = new AtomicInteger(
				0);
		final List<MemoryEntryRow> entryRows = MemoryStoreUtils.entryToRows(
				new MockComponents.MockAbstractDataAdapter(),
				new PrimaryIndex(
						new MockComponents.MockIndexStrategy(),
						new MockComponents.TestIndexModel()),
				new Integer(
						25),
				new IngestCallback<Integer>() {

					@Override
					public void entryIngested(
							final DataStoreEntryInfo entryInfo,
							final Integer entry ) {
						count.incrementAndGet();
					}
				},
				new VisibilityWriter<Integer>() {
					@Override
					public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
							final ByteArrayId fieldId ) {
						return new GlobalVisibilityHandler(
								"aaa&bbb");
					}
				});
		assertTrue(entryRows.size() == 1);
		assertTrue(count.get() == 1);
	}

	@Test
	public void testVisibility() {
		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa&(ccc|eee)".getBytes(),
				new String[] {
					"aaa",
					"eee",
					"xxx"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"eee",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"dddd",
					"ccc"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertTrue(MemoryStoreUtils.isAuthorized(
				"aaa".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertFalse(MemoryStoreUtils.isAuthorized(
				"xxx".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

	}

	protected static class TestStringAdapter extends
			AbstractDataAdapter<String> implements
			StatisticsProvider<String>
	{

		public TestStringAdapter() {}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"123");
		}

		@Override
		public boolean isSupported(
				final String entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final String entry ) {
			return new ByteArrayId(
					entry.getBytes());
		}

		@Override
		public FieldReader<Object> getReader(
				final ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public FieldWriter<String, Object> getWriter(
				final ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsTypes() {
			return null;
		}

		@Override
		public DataStatistics<String> createDataStatistics(
				final ByteArrayId statisticsId ) {
			return null;
		}

		@Override
		public EntryVisibilityHandler<String> getVisibilityHandler(
				final ByteArrayId statisticsId ) {
			return null;
		}

		@Override
		protected RowBuilder<String, Object> newBuilder() {
			return null;
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final ByteArrayId fieldId ) {
			return -1;
		}

		@Override
		public ByteArrayId getFieldIdForPosition(
				final CommonIndexModel model,
				final int position ) {
			return null;
		}
	}
}
