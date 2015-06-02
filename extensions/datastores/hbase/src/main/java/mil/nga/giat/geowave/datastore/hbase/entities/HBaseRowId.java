/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.entities;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author viggy Functionality similar to <code> AccumuloRowId </code>
 */
public class HBaseRowId
{

	private byte[] insertionId;
	private byte[] dataId;
	private byte[] adapterId;
	private int numberOfDuplicates;

	public HBaseRowId(
			final byte[] hbaseRowId ) {
		final byte[] metadata = Arrays.copyOfRange(
				hbaseRowId,
				hbaseRowId.length - 12,
				hbaseRowId.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				hbaseRowId,
				0,
				hbaseRowId.length - 12);
		final byte[] insertionId = new byte[hbaseRowId.length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(insertionId);
		buf.get(adapterId);
		buf.get(dataId);
		this.insertionId = insertionId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public HBaseRowId(
			final byte[] indexId,
			final byte[] dataId,
			final byte[] adapterId,
			final int numberOfDuplicates ) {
		this.insertionId = indexId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public byte[] getRowId() {
		final ByteBuffer buf = ByteBuffer.allocate(12 + dataId.length + adapterId.length + insertionId.length);
		buf.put(insertionId);
		buf.put(adapterId);
		buf.put(dataId);
		buf.putInt(adapterId.length);
		buf.putInt(dataId.length);
		buf.putInt(numberOfDuplicates);
		return buf.array();
	}

	public byte[] getInsertionId() {
		return insertionId;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public byte[] getAdapterId() {
		return adapterId;
	}

	public int getNumberOfDuplicates() {
		return numberOfDuplicates;
	}

	public boolean isDeduplicationEnabled() {
		return numberOfDuplicates >= 0;
	}
}
