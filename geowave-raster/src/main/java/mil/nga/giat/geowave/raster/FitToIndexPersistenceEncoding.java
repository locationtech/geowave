package mil.nga.giat.geowave.raster;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.data.PersistentDataset;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

public class FitToIndexPersistenceEncoding extends
		AdapterPersistenceEncoding
{
	private final List<ByteArrayId> insertionIds = new ArrayList<ByteArrayId>();

	public FitToIndexPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<? extends CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData,
			final ByteArrayId insertionId ) {
		super(
				adapterId,
				dataId,
				commonData,
				adapterExtendedData);
		insertionIds.add(insertionId);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final Index index ) {
		return insertionIds;
	}

	@Override
	public boolean isDeduplicationEnabled() {
		return false;
	}

}
