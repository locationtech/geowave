package mil.nga.giat.geowave.core.store.entities;

import java.nio.ByteBuffer;

public interface NativeGeoWaveRow
{
	public ByteBuffer getAdapterAndDataId();

	public ByteBuffer getValue();

	public ByteBuffer getIndex();
}
