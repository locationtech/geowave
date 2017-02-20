package mil.nga.giat.geowave.core.store.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class MemoryStoreUtilsTest
{
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
}
