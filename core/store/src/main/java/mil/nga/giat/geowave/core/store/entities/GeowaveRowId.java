package mil.nga.giat.geowave.core.store.entities;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class encapsulates the elements that compose the row ID in the
 * datastore, and can serialize and deserialize the individual elements to/from
 * the row ID. The row ID consists of the index ID, followed by an adapter ID,
 * followed by a data ID, followed by data ID length and adapter ID length, and
 * lastly the number of duplicate row IDs for this entry. The data ID must be
 * unique for an adapter, so the combination of adapter ID and data ID is
 * intended to guarantee uniqueness for this row ID.
 * 
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "private class only accessed internally")
public class GeowaveRowId
{
	private final byte[] insertionId;
	private final byte[] dataId;
	private final byte[] adapterId;
	private final int numberOfDuplicates;

	public GeowaveRowId(
			final byte[] rowId ) {
		final byte[] metadata = Arrays.copyOfRange(
				rowId,
				rowId.length - 12,
				rowId.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rowId,
				0,
				rowId.length - 12);
		final byte[] insertionId = new byte[rowId.length - 12 - adapterIdLength - dataIdLength];
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

	public GeowaveRowId(
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
