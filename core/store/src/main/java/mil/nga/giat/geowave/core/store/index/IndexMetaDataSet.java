package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.SortedIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class IndexMetaDataSet<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	private List<IndexMetaData> metaData;
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"INDEX_METADATA_");

	protected IndexMetaDataSet() {}

	private IndexMetaDataSet(
			final ByteArrayId adapterId,
			final ByteArrayId statsId,
			final List<IndexMetaData> metaData ) {
		super(
				adapterId,
				statsId);
		this.metaData = metaData;
	}

	public IndexMetaDataSet(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final SortedIndexStrategy<?, ?> indexStrategy ) {
		super(
				adapterId,
				composeId(indexId));
		this.metaData = indexStrategy.createMetaData();
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID.getBytes(),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new IndexMetaDataSet<T>(
				dataAdapterId,
				statisticsId,
				this.metaData);
	}

	public List<IndexMetaData> getMetaData() {
		return metaData;
	}

	@Override
	public byte[] toBinary() {
		final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
		final ByteBuffer buffer = super.binaryBuffer(metaBytes.length);
		buffer.put(metaBytes);
		return buffer.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
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
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof IndexMetaDataSet)) {
			for (int i = 0; i < metaData.size(); i++) {
				metaData.get(
						i).merge(
						((IndexMetaDataSet<T>) merge).metaData.get(i));
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (!this.metaData.isEmpty()) {
			final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(kvs);
			for (final IndexMetaData imd : this.metaData) {
				imd.insertionIdsAdded(insertionIds);
			}
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (!this.metaData.isEmpty()) {
			final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(kvs);
			for (final IndexMetaData imd : this.metaData) {
				imd.insertionIdsRemoved(insertionIds);
			}
		}
	}

	public static IndexMetaData[] getIndexMetadata(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		IndexMetaDataSet combinedMetaData = null;
		for (final ByteArrayId adapterId : adapterIdsToQuery) {
			final IndexMetaDataSet adapterMetadata = (IndexMetaDataSet) statisticsStore.getDataStatistics(
					adapterId,
					IndexMetaDataSet.composeId(index.getId()),
					authorizations);
			if (combinedMetaData == null) {
				combinedMetaData = adapterMetadata;
			}
			else {
				combinedMetaData.merge(adapterMetadata);
			}
		}
		return combinedMetaData != null ? combinedMetaData.toArray() : null;
	}
}
