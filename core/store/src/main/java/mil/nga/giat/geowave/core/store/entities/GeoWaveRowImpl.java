package mil.nga.giat.geowave.core.store.entities;

public class GeoWaveRowImpl implements
		GeoWaveRow
{
	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	public GeoWaveRowImpl(
			final GeoWaveKey key,
			final GeoWaveValue[] fieldValues ) {
		this.key = key;
		this.fieldValues = fieldValues;
	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public short getInternalAdapterId() {
		return key.getInternalAdapterId();
	}

	@Override
	public byte[] getSortKey() {
		return key.getSortKey();
	}

	@Override
	public byte[] getPartitionKey() {
		return key.getPartitionKey();
	}

	@Override
	public int getNumberOfDuplicates() {
		return key.getNumberOfDuplicates();
	}

	public GeoWaveKey getKey() {
		return key;
	}

	@Override
	public GeoWaveValue[] getFieldValues() {
		return fieldValues;
	}
}
