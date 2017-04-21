package mil.nga.giat.geowave.core.store.entities;

public interface NativeGeoWaveRow
{
	public byte[] getDataId();

	public byte[] getAdapterId();

	public byte[] getFieldMask();

	public byte[] getValue();

	public byte[] getIndex();
}
