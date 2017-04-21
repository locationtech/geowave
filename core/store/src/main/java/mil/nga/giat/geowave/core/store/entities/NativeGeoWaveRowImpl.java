package mil.nga.giat.geowave.core.store.entities;

public class NativeGeoWaveRowImpl implements
		NativeGeoWaveRow
{
	private final byte[] dataId;
	private final byte[] adapterId;
	private final byte[] index;
	private final byte[] value;
	private final byte[] fieldMask;

	public NativeGeoWaveRowImpl(
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] index,
			final byte[] fieldMask,
			final byte[] value ) {
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.index = index;
		this.fieldMask = fieldMask;
		this.value = value;
	}

	@Override
	public byte[] getFieldMask() {
		return fieldMask;
	}

	@Override
	public byte[] getDataId() {
		return dataId;
	}

	@Override
	public byte[] getAdapterId() {
		return adapterId;
	}

	@Override
	public byte[] getIndex() {
		return index;
	}

	@Override
	public byte[] getValue() {
		return value;
	}
}
