package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

/**
 * One manager associated with each primary index.
 *
 *
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexDataManager<T> implements
		Closeable,
		IngestCallback<T>,
		DeleteCallback<T, GeoWaveRow>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	private final SecondaryIndexDataStore secondaryIndexStore;
	private final CommonIndexModel primaryIndexModel;
	private final ByteArrayId primaryIndexId;

	public SecondaryIndexDataManager(
			final SecondaryIndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final PrimaryIndex primaryIndex ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexModel = primaryIndex.getIndexModel();
		this.primaryIndexId = primaryIndex.getId();

	}

	public void entryCallback(
			final T entry,
			final boolean delete,
			final GeoWaveRow... kvs ) {
		// loop secondary indices for adapter
		final InsertionIds primaryIndexInsertionIds = DataStoreUtils.keysToInsertionIds(kvs);
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			final int position = adapter.getPositionOfOrderedField(
					primaryIndexModel,
					indexedAttributeFieldId);
			Object fieldValue = null;
			byte[] visibility = null;
			// find the field value and deserialize it
			for (final GeoWaveValue v : kvs[0].getFieldValues()) {
				if (BitmaskUtils.getFieldPositions(
						v.getFieldMask()).contains(
						position)) {
					final byte[] fieldSubsetBitmask = BitmaskUtils.generateCompositeBitmask(position);

					final byte[] byteValue = BitmaskUtils.constructNewValue(
							v.getValue(),
							v.getFieldMask(),
							fieldSubsetBitmask);
					fieldValue = adapter.getReader(
							indexedAttributeFieldId).readField(
							byteValue);
					visibility = v.getVisibility();
					break;
				}
			}
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final InsertionIds secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					fieldValue);
			// loop insertionIds
			for (final ByteArrayId insertionId : secondaryIndexInsertionIds.getCompositeInsertionIds()) {
				final ByteArrayId dataId = new ByteArrayId(
						kvs[0].getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						final Pair<ByteArrayId, ByteArrayId> firstPartitionAndSortKey = primaryIndexInsertionIds
								.getFirstPartitionAndSortKeyPair();
						if (delete) {
							secondaryIndexStore.storeJoinEntry(
									secondaryIndex.getId(),
									insertionId,
									adapter.getAdapterId(),
									indexedAttributeFieldId,
									primaryIndexId,
									firstPartitionAndSortKey.getLeft(),
									firstPartitionAndSortKey.getRight(),
									new ByteArrayId(
											visibility));
						}
						else {
							secondaryIndexStore.deleteJoinEntry(
									secondaryIndex.getId(),
									insertionId,
									adapter.getAdapterId(),
									indexedAttributeFieldId,
									firstPartitionAndSortKey.getLeft(),
									firstPartitionAndSortKey.getRight(),
									new ByteArrayId(
											visibility));
						}
						break;
					case PARTIAL:
						final List<ByteArrayId> attributesToStore = secondaryIndex.getPartialFieldIds();

						final byte[] fieldSubsetBitmask = BitmaskUtils.generateFieldSubsetBitmask(
								primaryIndexModel,
								attributesToStore,
								adapter);
						final List<GeoWaveValue> subsetValues = new ArrayList<>();
						for (final GeoWaveValue value : kvs[0].getFieldValues()) {
							byte[] byteValue = value.getValue();
							byte[] fieldMask = value.getFieldMask();

							if (fieldSubsetBitmask != null) {
								final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
										fieldMask,
										fieldSubsetBitmask);
								byteValue = BitmaskUtils.constructNewValue(
										byteValue,
										fieldMask,
										newBitmask);
								if ((byteValue == null) || (byteValue.length == 0)) {
									continue;
								}
								fieldMask = newBitmask;
							}
							subsetValues.add(new GeoWaveValueImpl(
									fieldMask,
									value.getVisibility(),
									byteValue));
						}
						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								subsetValues.toArray(new GeoWaveValue[] {}));
						break;
					case FULL:
						// assume multiple rows are duplicates, so just take the
						// first one

						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								// full simply sends over all of the
								// attributes
								// kvs is gauranteed to be at least one, or
								// there would have been nothing ingested
								kvs[0].getFieldValues());
						break;
					default:
						break;
				}
			}
			if (delete) {
				// capture statistics
				for (final DataStatistics<T> associatedStatistic : secondaryIndex.getAssociatedStatistics()) {
					associatedStatistic.entryIngested(
							entry,
							kvs);
				}
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		entryCallback(
				entry,
				false,
				kvs);
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		entryCallback(
				entry,
				true,
				kvs);

	}

	@Override
	public void close()
			throws IOException {
		if (secondaryIndexStore != null) {
			secondaryIndexStore.flush();
		}
	}

}