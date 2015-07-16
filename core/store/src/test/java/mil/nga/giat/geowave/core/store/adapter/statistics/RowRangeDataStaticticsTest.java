package mil.nga.giat.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

import org.junit.Test;

public class RowRangeDataStaticticsTest
{

	@Test
	public void testEmpty() {
		RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		assertFalse(stats.isSet());

		stats.fromBinary(stats.toBinary());
	}

	@Test
	public void testIngest() {
		RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		stats.entryIngested(
				new DataStoreEntryInfo(
						Arrays.asList(
								new ByteArrayId(
										"20030"),
								new ByteArrayId(
										"014"),
								new ByteArrayId(
										"0124"),
								new ByteArrayId(
										"0123"),
								new ByteArrayId(
										"5064"),
								new ByteArrayId(
										"50632")),
						Collections.<FieldInfo> emptyList()),
				1);

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"5064").getBytes(),
				stats.getMax()));

		assertTrue(stats.isSet());

		// merge

		RowRangeDataStatistics<Integer> stats2 = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		stats2.entryIngested(
				new DataStoreEntryInfo(
						Arrays.asList(
								new ByteArrayId(
										"20030"),
								new ByteArrayId(
										"014"),
								new ByteArrayId(
										"8062")),
						Collections.<FieldInfo> emptyList()),
				1);

		stats.merge(stats2);

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"8062").getBytes(),
				stats.getMax()));

		stats2.fromBinary(stats.toBinary());

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats2.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"8062").getBytes(),
				stats2.getMax()));

		stats.toString();
	}
}
