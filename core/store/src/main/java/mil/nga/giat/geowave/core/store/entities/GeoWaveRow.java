package mil.nga.giat.geowave.core.store.entities;

public interface GeoWaveRow extends
		GeoWaveKey
{
	public GeoWaveValue[] getFieldValues();
}
