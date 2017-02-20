package mil.nga.giat.geowave.core.store.entities;

public interface GeoWaveValue
{
	public byte[] getFieldMask();

	public byte[] getVisibility();

	public byte[] getValue();
}
