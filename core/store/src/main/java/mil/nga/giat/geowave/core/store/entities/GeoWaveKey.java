package mil.nga.giat.geowave.core.store.entities;

import java.nio.ByteBuffer;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public byte[] getAdapterId();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public int getNumberOfDuplicates();
	
	public static byte[] getCompositeId(GeoWaveKey key){
		final ByteBuffer buffer = ByteBuffer.allocate(
				key.getPartitionKey().length + key.getSortKey().length + key.getAdapterId().length + key.getDataId().length + 12);
		buffer.put(
				key.getPartitionKey());
		buffer.put(
				key.getSortKey());
		buffer.put(
				key.getAdapterId());
		buffer.put(
				key.getDataId());
		buffer.putInt(
				key.getAdapterId().length);
		buffer.putInt(
				key.getDataId().length);
		buffer.putInt(
				key.getNumberOfDuplicates());
		return buffer.array();
	}
}
