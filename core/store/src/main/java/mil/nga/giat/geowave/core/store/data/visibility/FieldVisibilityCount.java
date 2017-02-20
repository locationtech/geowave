package mil.nga.giat.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class FieldVisibilityCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"FIELD_VISIBILITY_COUNT");
	private static final ByteArrayId SEPARATOR = new ByteArrayId(
			"_");
	private static final byte[] STATS_ID_AND_SEPARATOR = ArrayUtils.addAll(
			STATS_ID.getBytes(),
			SEPARATOR.getBytes());
	private final Map<ByteArrayId, Long> countsPerVisibility;

	protected FieldVisibilityCount() {
		super();
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	private FieldVisibilityCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statsId,
			final Map<ByteArrayId, Long> countsPerVisibility ) {
		super(
				dataAdapterId,
				statsId);
		this.countsPerVisibility = countsPerVisibility;
	}

	public FieldVisibilityCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId ) {
		super(
				dataAdapterId,
				composeId(indexId));
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID_AND_SEPARATOR,
						indexId.getBytes()));
	}

	@Override
	public byte[] toBinary() {
		int bufferSize = 4;
		final List<byte[]> serializedCounts = new ArrayList<byte[]>();
		for (final Entry<ByteArrayId, Long> entry : countsPerVisibility.entrySet()) {
			final byte[] key = entry.getKey().getBytes();
			final ByteBuffer buf = ByteBuffer.allocate(key.length + 12);
			buf.putInt(key.length);
			buf.put(key);
			buf.putLong(entry.getValue());
			byte[] serializedEntry = buf.array();
			serializedCounts.add(serializedEntry);
			bufferSize += serializedEntry.length;
		}
		ByteBuffer buf = super.binaryBuffer(bufferSize);
		buf.putInt(serializedCounts.size());
		for (byte[] count : serializedCounts) {
			buf.put(count);
		}
		return buf.array();
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new FieldVisibilityCount<T>(
				dataAdapterId,
				statisticsId,
				this.countsPerVisibility);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		final int size = buf.getInt();
		countsPerVisibility.clear();
		for (int i = 0; i < size; i++) {
			final int idCount = buf.getInt();
			final byte[] id = new byte[idCount];
			buf.get(id);
			final long count = buf.getLong();
			countsPerVisibility.put(
					new ByteArrayId(
							id),
					count);
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			GeoWaveRow... kvs ) {
		updateEntry(
				1,
				kvs);
	}

	private void updateEntry(
			final int incrementValue,
			GeoWaveRow... kvs ) {
		for (GeoWaveRow row : kvs) {
			GeoWaveValue[] values = row.getFieldValues();
			for (final GeoWaveValue v : values) {
				ByteArrayId visibility = new ByteArrayId(
						new byte[] {});
				if (v.getVisibility() != null) {
					visibility = new ByteArrayId(
							v.getVisibility());
				}
				Long count = countsPerVisibility.get(visibility);
				if (count == null) {
					count = 0L;
				}
				countsPerVisibility.put(
						visibility,
						count + incrementValue);
			}
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			GeoWaveRow... kvs ) {
		updateEntry(
				-1,
				kvs);
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof FieldVisibilityCount)) {
			final Map<ByteArrayId, Long> otherCounts = ((FieldVisibilityCount) merge).countsPerVisibility;
			for (final Entry<ByteArrayId, Long> entry : otherCounts.entrySet()) {
				Long count = countsPerVisibility.get(entry.getKey());
				if (count == null) {
					count = 0L;
				}
				countsPerVisibility.put(
						entry.getKey(),
						count + entry.getValue());
			}
		}
	}
}
