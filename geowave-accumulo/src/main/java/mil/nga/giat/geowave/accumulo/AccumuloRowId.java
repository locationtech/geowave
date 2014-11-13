package mil.nga.giat.geowave.accumulo;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.accumulo.core.data.Key;

/**
 * This class encapsulates the elements that compose the row ID in Accumulo, and
 * can serialize and deserialize the individual elements to/from the row ID. The
 * row ID consists of the index ID, followed by an adapter ID, followed by a
 * data ID, followed by data ID length and adapter ID length, and lastly the
 * number of duplicate row IDs for this entry. The data ID must be unique for an
 * adapter, so the combination of adapter ID and data ID is intended to
 * guarantee uniqueness for this row ID.
 *
 */
public class AccumuloRowId
{
	private final byte[] indexId;
	private final byte[] dataId;
	private final byte[] adapterId;
	private final int numberOfDuplicates;

	public AccumuloRowId(
			final Key key ) {
		this(
				key.getRow().copyBytes());
	}

	public AccumuloRowId(
			final byte[] accumuloRowId ) {
		final byte[] metadata = Arrays.copyOfRange(
				accumuloRowId,
				accumuloRowId.length - 12,
				accumuloRowId.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				accumuloRowId,
				0,
				accumuloRowId.length - 12);
		final byte[] indexId = new byte[accumuloRowId.length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(indexId);
		buf.get(adapterId);
		buf.get(dataId);
		this.indexId = indexId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public AccumuloRowId(
			final byte[] indexId,
			final byte[] dataId,
			final byte[] adapterId,
			final int numberOfDuplicates ) {
		this.indexId = indexId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public byte[] getRowId() {
		final ByteBuffer buf = ByteBuffer.allocate(12 + dataId.length + adapterId.length + indexId.length);
		buf.put(indexId);
		buf.put(adapterId);
		buf.put(dataId);
		buf.putInt(adapterId.length);
		buf.putInt(dataId.length);
		buf.putInt(numberOfDuplicates);
		return buf.array();
	}

	public byte[] getIndexId() {
		return indexId;
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
}
