package mil.nga.giat.geowave.core.store.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.EntryRow;

import org.junit.Test;

public class DataStoreUtilsTest
{
	@Test
	public void testEntryToRows() {
		final AtomicInteger count = new AtomicInteger(
				0);
		List<EntryRow> entryRows = DataStoreUtils.entryToRows(
				new MockComponents.MockAbstractDataAdapter(),
				new PrimaryIndex(
						new MockComponents.MockIndexStrategy(),
						new MockComponents.TestIndexModel()),
				new Integer(
						25),
				new IngestCallback<Integer>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							Integer entry ) {
						count.incrementAndGet();
					}
				},
				new VisibilityWriter<Integer>() {
					@Override
					public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
							ByteArrayId fieldId ) {
						return new GlobalVisibilityHandler(
								"aaa&bbb");
					}
				});
		assertTrue(entryRows.size() == 1);
		assertTrue(count.get() == 1);
	}

	@Test
	public void testVisibility() {
		assertTrue(DataStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb",
					"ccc"
				}));

		assertFalse(DataStoreUtils.isAuthorized(
				"aaa&ccc".getBytes(),
				new String[] {
					"aaa",
					"bbb"
				}));

		assertTrue(DataStoreUtils.isAuthorized(
				"aaa&(ccc|eee)".getBytes(),
				new String[] {
					"aaa",
					"eee",
					"xxx"
				}));

		assertTrue(DataStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"eee",
					"ccc"
				}));

		assertFalse(DataStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"bbb",
					"dddd",
					"ccc"
				}));

		assertTrue(DataStoreUtils.isAuthorized(
				"aaa|(ccc&eee)".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertTrue(DataStoreUtils.isAuthorized(
				"aaa".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

		assertFalse(DataStoreUtils.isAuthorized(
				"xxx".getBytes(),
				new String[] {
					"aaa",
					"dddd",
					"ccc"
				}));

	}

	protected static class TestStringAdapter extends
			AbstractDataAdapter<String> implements
			StatisticalDataAdapter<String>
	{

		public TestStringAdapter() {}

		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"123");
		}

		@Override
		public boolean isSupported(
				String entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				String entry ) {
			return new ByteArrayId(
					entry.getBytes());
		}

		@Override
		public FieldReader<Object> getReader(
				ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public FieldWriter<String, Object> getWriter(
				ByteArrayId fieldId ) {
			return null;
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsIds() {
			return null;
		}

		@Override
		public DataStatistics<String> createDataStatistics(
				ByteArrayId statisticsId ) {
			return null;
		}

		@Override
		public EntryVisibilityHandler<String> getVisibilityHandler(
				ByteArrayId statisticsId ) {
			return null;
		}

		@Override
		protected RowBuilder<String, Object> newBuilder() {
			return null;
		}
	}
}
