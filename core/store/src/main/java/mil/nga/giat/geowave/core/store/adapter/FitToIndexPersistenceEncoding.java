package mil.nga.giat.geowave.core.store.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class FitToIndexPersistenceEncoding extends
		AdapterPersistenceEncoding
{
	private final InsertionIds insertionIds;

	public FitToIndexPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData,
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		super(
				adapterId,
				dataId,
				commonData,
				adapterExtendedData);
		insertionIds = new InsertionIds(
				partitionKey,
				sortKey == null ? null : Collections.singletonList(sortKey));
	}

	@Override
	public InsertionIds getInsertionIds(
			final PrimaryIndex index ) {
		return insertionIds;
	}

	@Override
	public boolean isDeduplicationEnabled() {
		return false;
	}

}
