package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;

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
		DeleteCallback<T>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	final SecondaryIndexDataStore secondaryIndexStore;
	final ByteArrayId primaryIndexId;
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";

	public SecondaryIndexDataManager(
			final SecondaryIndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexId = primaryIndexId;

	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		// loop secondary indices for adapter
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId secondaryIndexId = new ByteArrayId(
					TABLE_PREFIX + secondaryIndex.getId().getString());
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			// get fieldInfo for fieldId to be indexed
			final FieldInfo<?> indexedAttributeFieldInfo = getFieldInfo(
					entryInfo,
					indexedAttributeFieldId);
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					Arrays.asList(indexedAttributeFieldInfo));
			// loop insertionIds
			for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
				final ByteArrayId primaryIndexRowId = entryInfo.getRowIds().get(
						0);
				final ByteArrayId attributeVisibility = new ByteArrayId(
						indexedAttributeFieldInfo.getVisibility());
				final ByteArrayId dataId = new ByteArrayId(
						entryInfo.getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						secondaryIndexStore.storeJoinEntry(
								secondaryIndexId,
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								primaryIndexId,
								primaryIndexRowId,
								attributeVisibility);
						break;
					case PARTIAL:
						final List<FieldInfo<?>> attributes = new ArrayList<>();
						final List<ByteArrayId> attributesToStore = secondaryIndex.getPartialFieldIds();
						for (final ByteArrayId fieldId : attributesToStore) {
							attributes.add(getFieldInfo(
									entryInfo,
									fieldId));
						}
						secondaryIndexStore.storeEntry(
								secondaryIndexId,
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								attributes);
						break;
					case FULL:
						secondaryIndexStore.storeEntry(
								secondaryIndexId,
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								// full simply sends over all of the
								// attributes
								entryInfo.getFieldInfo());
						break;
					default:
						break;
				}
			}
			// capture statistics
			for (final DataStatistics<T> associatedStatistic : secondaryIndex.getAssociatedStatistics()) {
				associatedStatistic.entryIngested(
						entryInfo,
						entry);
			}
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {

		for (final SecondaryIndex<T> index : adapter.getSupportedSecondaryIndices()) {
			final List<FieldInfo<?>> indexedAttributes = new LinkedList<FieldInfo<?>>();
			indexedAttributes.add(getFieldInfo(
					entryInfo,
					index.getFieldId()));
			secondaryIndexStore.delete(
					index,
					indexedAttributes);
		}

	}

	private FieldInfo<?> getFieldInfo(
			final DataStoreEntryInfo entryInfo,
			final ByteArrayId fieldID ) {
		for (final FieldInfo<?> info : entryInfo.getFieldInfo()) {
			if (info.getDataValue().getId().equals(
					fieldID)) {
				return info;
			}
		}
		return null;
	}

	@Override
	public void close()
			throws IOException {
		if (secondaryIndexStore != null) {
			secondaryIndexStore.flush();
		}
	}

}