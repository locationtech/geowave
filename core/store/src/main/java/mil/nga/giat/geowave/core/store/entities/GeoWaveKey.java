package mil.nga.giat.geowave.core.store.entities;

import java.nio.ByteBuffer;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public short getInternalAdapterId();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public int getNumberOfDuplicates();
	
	public static byte[] getCompositeId(GeoWaveKey key){
		final ByteBuffer buffer = ByteBuffer.allocate(
				key.getPartitionKey().length + key.getSortKey().length + key.getDataId().length + 6);
		buffer.put(
				key.getPartitionKey());
		buffer.put(
				key.getSortKey());
		buffer.putShort(
				key.getInternalAdapterId());
		buffer.put(
				key.getDataId());
		buffer.putShort((short)
				key.getDataId().length);
		buffer.putShort(
				(short)key.getNumberOfDuplicates());
		buffer.rewind();
		return buffer.array();
	}
}
