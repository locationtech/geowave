package mil.nga.giat.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

public class PartitionStatisticsTest
{
	static final long base = 7l;
	static int counter = 0;

	private GeoWaveKey genKey(
			final long id ) {
		final InsertionIds insertionIds = new InsertionIds(
				new ByteArrayId(
						new byte[] {
							(byte) (counter++ % 32)
						}),
				Arrays.asList(new ByteArrayId(
						String.format(
								"\12%5h",
								base + id) + "20030f89")));
		return GeoWaveKeyImpl.createKeys(
				insertionIds,
				new byte[] {},
				new byte[] {})[0];
	}

	@Test
	public void testIngest() {
		final PartitionStatistics<Integer> stats = new PartitionStatistics<Integer>(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"20030"));

		for (long i = 0; i < 10000; i++) {
			final GeoWaveRow row = new GeoWaveRowImpl(
					genKey(i),
					new GeoWaveValue[] {});
			stats.entryIngested(
					1,
					row);
		}

		System.out.println(stats.toString());

		assertEquals(
				32,
				stats.getPartitionKeys().size());
		for (byte i = 0; i < 32; i++) {
			Assert.assertTrue(stats.getPartitionKeys().contains(
					new ByteArrayId(
							new byte[] {
								i
							})));
		}
	}
}
