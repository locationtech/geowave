package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.DeleteCallback;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

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

		for (final SecondaryIndex<T> index : adapter.getSupportedSecondaryIndices()) {
			final List<FieldInfo<?>> indexedAttributes = new LinkedList<FieldInfo<?>>();
			for (final ByteArrayId fieldID : index.getFieldIDs()) {
				indexedAttributes.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
			secondaryIndexStore.store(
					index,
					primaryIndexId,
					entryInfo.getRowIds().get(
							0),
					indexedAttributes);

			List<DataStatistics<T>> associatedStatistics = index.getAssociatedStatistics();
			for (DataStatistics<T> associatedStatistic : associatedStatistics) {
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
			for (final ByteArrayId fieldID : index.getFieldIDs()) {
				indexedAttributes.add(getFieldInfo(
						entryInfo,
						fieldID));
			}
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
		secondaryIndexStore.flush();
	}

}
