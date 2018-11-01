package org.locationtech.geowave.datastore.redis.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Streams;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

public class RedisUtils
{
	public static int REDIS_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
	public static int REDIS_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;

	public static RScoredSortedSet<GeoWaveMetadata> getMetadataSet(
			final RedissonClient client,
			final String namespace,
			final MetadataType metadataType ) {
		return client.getScoredSortedSet(
				namespace + "_" + metadataType.toString(),
				GeoWaveMetadataCodec.SINGLETON);
	}

	public static String getRowSetPrefix(
			final String namespace,
			final String typeName,
			final String indexName ) {
		return namespace + "_" + typeName + "_" + indexName;
	}

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String setNamePrefix,
			final byte[] partitionKey,
			final boolean requiresTimestamp ) {
		return getRowSet(
				client,
				getRowSetName(
						setNamePrefix,
						partitionKey),
				requiresTimestamp);

	}

	public static String getRowSetName(
			final String namespace,
			final String typeName,
			final String indexName,
			final byte[] partitionKey ) {
		return getRowSetName(
				getRowSetPrefix(
						namespace,
						typeName,
						indexName),
				partitionKey);
	}

	public static String getRowSetName(
			final String setNamePrefix,
			final byte[] partitionKey ) {
		String partitionStr;
		if ((partitionKey != null) && (partitionKey.length > 0)) {
			partitionStr = "_" + ByteArrayUtils.byteArrayToString(partitionKey);
		}
		else {
			partitionStr = "";
		}
		return setNamePrefix + partitionStr;
	}

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String setName,
			final boolean requiresTimestamp ) {
		return client.getScoredSortedSet(
				setName,
				requiresTimestamp ? GeoWaveRedisRowWithTimestampCodec.SINGLETON : GeoWaveRedisRowCodec.SINGLETON);

	}

	public static RScoredSortedSet<GeoWaveRedisPersistedRow> getRowSet(
			final RedissonClient client,
			final String namespace,
			final String typeName,
			final String indexName,
			final byte[] partitionKey,
			final boolean requiresTimestamp ) {
		return getRowSet(
				client,
				getRowSetPrefix(
						namespace,
						typeName,
						indexName),
				partitionKey,
				requiresTimestamp);
	}

	public static double getScore(
			final byte[] byteArray ) {
		return bytesToLong(byteArray);
	}

	public static byte[] getSortKey(
			final double score ) {
		return longToBytes((long) score);
	}

	private static byte[] longToBytes(
			long val ) {

		final int radix = 1 << 8;
		final int mask = radix - 1;
		// we want to eliminate trailing 0's (ie. truncate the byte array by
		// trailing 0's)
		int trailingZeros = 0;
		while ((((int) val) & mask) == 0) {
			val >>>= 8;
			trailingZeros++;
			if (trailingZeros == 8) {
				return new byte[0];
			}
		}
		final byte[] array = new byte[8 - trailingZeros];
		int pos = array.length;
		do {
			array[--pos] = (byte) (((int) val) & mask);
			val >>>= 8;

		}
		while ((val != 0) && (pos > 0));

		return array;
	}

	private static long bytesToLong(
			final byte[] bytes ) {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			value = (value << 8);
			if (i < bytes.length) {
				value += (bytes[i] & 0xff);
			}
		}
		return value;
	}

	public static Set<ByteArray> getPartitions(
			final RedissonClient client,
			final String setNamePrefix ) {
		return Streams
				.stream(
						client
								.getKeys()
								.getKeysByPattern(
										setNamePrefix + "*"))
				.map(
						str -> str.length() > (setNamePrefix.length() + 1) ? new ByteArray(
								ByteArrayUtils
										.byteArrayFromString(
												str
														.substring(
																setNamePrefix.length() + 1)))
								: new ByteArray())
				.collect(
						Collectors.toSet());
	}

	public static Iterator<GeoWaveMetadata> groupByIds(
			final Iterable<GeoWaveMetadata> result ) {
		final ListMultimap<ByteArray, GeoWaveMetadata> multimap = MultimapBuilder.hashKeys().arrayListValues().build();
		result
				.forEach(
						r -> multimap
								.put(
										new ByteArray(
												Bytes
														.concat(
																r.getPrimaryId(),
																r.getSecondaryId())),
										r));
		return multimap.values().iterator();
	}

	public static Collection<ScoredEntry<GeoWaveRedisPersistedRow>> groupByRow(
			final Collection<ScoredEntry<GeoWaveRedisPersistedRow>> result,
			final boolean sortByTime ) {
		final ListMultimap<Pair<Double, ByteArray>, ScoredEntry<GeoWaveRedisPersistedRow>> multimap = MultimapBuilder
				.hashKeys()
				.arrayListValues()
				.build();
		result
				.forEach(
						r -> multimap
								.put(
										Pair
												.of(
														r.getScore(),
														new ByteArray(
																r.getValue().getDataId())),
										r));
		if (sortByTime) {
			multimap
					.asMap()
					.forEach(
							(
									k,
									v ) -> Collections
											.sort(
													(List<ScoredEntry<GeoWaveRedisPersistedRow>>) v,
													TIMESTAMP_COMPARATOR));
		}
		return multimap.values();
	}

	public static boolean isSortByTime(
			final InternalDataAdapter<?> adapter ) {
		return adapter.getAdapter() instanceof RowMergingDataAdapter;
	}

	public static boolean isSortByKeyRequired(
			final BaseReaderParams<?> params ) {
		// subsampling needs to be sorted by sort key to work properly
		return (params.getMaxResolutionSubsamplingPerDimension() != null)
				&& (params.getMaxResolutionSubsamplingPerDimension().length > 0);
	}

	public static Iterator<GeoWaveRow> sortBySortKey(
			final Iterator<GeoWaveRow> it ) {
		return Streams.stream(
				it).sorted(
				SortKeyOrder.SINGLETON).iterator();
	}

	public static Pair<Boolean, Boolean> isGroupByRowAndIsSortByTime(
			final BaseReaderParams<?> readerParams,
			final short adapterId ) {
		final boolean sortByTime = isSortByTime(readerParams.getAdapterStore().getAdapter(
				adapterId));
		return Pair.of(
				readerParams.isMixedVisibility() || sortByTime,
				sortByTime);
	}

	private static final ReverseTimestampComparator TIMESTAMP_COMPARATOR = new ReverseTimestampComparator();

	private static class ReverseTimestampComparator implements
			Comparator<ScoredEntry<GeoWaveRedisPersistedRow>>,
			Serializable
	{
		private static final long serialVersionUID = 2894647323275155231L;

		@Override
		public int compare(
				final ScoredEntry<GeoWaveRedisPersistedRow> o1,
				final ScoredEntry<GeoWaveRedisPersistedRow> o2 ) {
			final GeoWaveRedisPersistedTimestampRow row1 = (GeoWaveRedisPersistedTimestampRow) o1.getValue();
			final GeoWaveRedisPersistedTimestampRow row2 = (GeoWaveRedisPersistedTimestampRow) o2.getValue();
			// we are purposely reversing the order because we want it to be
			// sorted from most recent to least recent
			final int compare = Long.compare(
					row2.getSecondsSinceEpic(),
					row1.getSecondsSinceEpic());
			if (compare != 0) {
				return compare;
			}
			return Integer.compare(
					row2.getNanoOfSecond(),
					row1.getNanoOfSecond());
		}

	}

	private static class SortKeyOrder implements
			Comparator<GeoWaveRow>,
			Serializable
	{
		private static SortKeyOrder SINGLETON = new SortKeyOrder();
		private static final long serialVersionUID = 23275155231L;

		@Override
		public int compare(
				final GeoWaveRow o1,
				final GeoWaveRow o2 ) {
			if (o1 == o2) {
				return 0;
			}
			if (o1 == null) {
				return 1;
			}
			if (o2 == null) {
				return -1;
			}
			byte[] otherComp = o2.getSortKey() == null ? new byte[0] : o2.getSortKey();
			byte[] thisComp = o1.getSortKey() == null ? new byte[0] : o1.getSortKey();

			int comp = UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
			if (comp != 0) {
				return comp;
			}
			otherComp = o2.getPartitionKey() == null ? new byte[0] : o2.getPartitionKey();
			thisComp = o1.getPartitionKey() == null ? new byte[0] : o1.getPartitionKey();

			comp = UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
			if (comp != 0) {
				return comp;
			}
			comp = Short.compare(
					o1.getAdapterId(),
					o2.getAdapterId());
			if (comp != 0) {
				return comp;
			}
			otherComp = o2.getDataId() == null ? new byte[0] : o2.getDataId();
			thisComp = o1.getDataId() == null ? new byte[0] : o1.getDataId();

			comp = UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);

			if (comp != 0) {
				return comp;
			}
			return Integer.compare(
					o1.getNumberOfDuplicates(),
					o2.getNumberOfDuplicates());
		}

	}
}
