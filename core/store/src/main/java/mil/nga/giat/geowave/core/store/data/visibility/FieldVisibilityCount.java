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
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;

public class FieldVisibilityCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FIELD_VISIBILITY_COUNT");
	private final Map<ByteArrayId, Long> countsPerVisibility;

	protected FieldVisibilityCount() {
		super();
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	private FieldVisibilityCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId,
			final Map<ByteArrayId, Long> countsPerVisibility ) {
		super(
				dataAdapterId,
				composeId(statisticsId));
		this.countsPerVisibility = countsPerVisibility;
	}

	public FieldVisibilityCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				dataAdapterId,
				composeId(statisticsId));
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	public static ByteArrayId composeId(
			ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
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
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		updateEntry(
				entryInfo,
				1);
	}

	private void updateEntry(
			final DataStoreEntryInfo entryInfo,
			final int incrementValue ) {
		if ((entryInfo != null) && (entryInfo.getFieldInfo() != null)) {
			final List<FieldInfo<?>> fields = entryInfo.getFieldInfo();
			for (final FieldInfo<?> field : fields) {
				ByteArrayId visibility = new ByteArrayId(
						new byte[] {});
				if (field.getVisibility() != null) {
					visibility = new ByteArrayId(
							field.getVisibility());
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
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		updateEntry(
				entryInfo,
				-1);
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
