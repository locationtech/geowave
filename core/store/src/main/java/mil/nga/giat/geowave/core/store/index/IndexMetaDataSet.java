package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class IndexMetaDataSet<T> extends
		AbstractDataStatistics<T> implements
		Mergeable
{
	private List<IndexMetaData> metaData;
	private ByteArrayId indexId;
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"INDEX_METADATA_");

	public IndexMetaDataSet() {}

	public IndexMetaDataSet(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final List<IndexMetaData> metaData ) {
		super(
				adapterId,
				composeId(indexId));
		this.indexId = indexId;
		this.metaData = metaData;
	}

	public static ByteArrayId composeId(
			ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID.getBytes(),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new IndexMetaDataSet<T>(
				dataAdapterId,
				decomposeFromId(statisticsId),
				this.metaData);
	}

	public static ByteArrayId decomposeFromId(
			final ByteArrayId id ) {
		int idLength = id.getBytes().length - STATS_ID.getBytes().length;
		byte[] idBytes = new byte[idLength];
		System.arraycopy(
				id.getBytes(),
				STATS_ID.getBytes().length,
				idBytes,
				0,
				idLength);
		return new ByteArrayId(
				idBytes);
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}

	public List<IndexMetaData> getMetaData() {
		return metaData;
	}

	@Override
	public byte[] toBinary() {
		final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
		ByteBuffer buffer = super.binaryBuffer(metaBytes.length);
		buffer.put(metaBytes);
		return buffer.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] metaBytes = new byte[buffer.remaining()];
		buffer.get(metaBytes);

		metaData = (List) PersistenceUtils.fromBinary(metaBytes);
	}

	public IndexMetaData[] toArray() {
		return metaData.toArray(new IndexMetaData[metaData.size()]);
	}

	@Override
	public void merge(
			Mergeable merge ) {
		if (merge != null && merge instanceof IndexMetaDataSet) {
			for (int i = 0; i < metaData.size(); i++) {
				metaData.get(
						i).merge(
						((IndexMetaDataSet<T>) merge).metaData.get(i));
			}
		}
	}

	@Override
	public void entryIngested(
			DataStoreEntryInfo entryInfo,
			T entry ) {
		for (IndexMetaData imd : this.metaData) {
			imd.update(entryInfo.getRowIds());
		}
	}

}
