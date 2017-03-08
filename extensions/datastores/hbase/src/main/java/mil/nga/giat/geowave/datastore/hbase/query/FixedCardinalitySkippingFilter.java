package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;

public class FixedCardinalitySkippingFilter extends
		FilterBase
{
	private Integer bitPosition;
	private byte[] nextRow = null;
	private boolean init = false;

	public FixedCardinalitySkippingFilter() {}

	public FixedCardinalitySkippingFilter(
			final Integer bitPosition ) {
		this.bitPosition = bitPosition;
	}

	@Override
	public Cell getNextCellHint(
			Cell currentKV )
			throws IOException {
		if (nextRow != null) {
			return KeyValueUtil.createFirstOnRow(nextRow);
		}
		
		return super.getNextCellHint(currentKV);
	}

	@Override
	public ReturnCode filterKeyValue(
			final Cell cell )
			throws IOException {
		// Retrieve the row key
		GeoWaveRowImpl geowaveRow = new GeoWaveRowImpl(
				cell.getRowArray(),
				cell.getRowOffset(),
				cell.getRowLength());
		
		final byte[] row = geowaveRow.getRowId();
		
		// Make sure we have the next row to include
		if (!init) {
			init = true;
			getNextRowKey(
					geowaveRow.getRowId());
		}

		// Compare current row w/ next row
		ReturnCode returnCode = checkNextRow(
				row);
		
		// If we're at or past the next row, advance it
		if (returnCode == ReturnCode.INCLUDE) {
			getNextRowKey(
					geowaveRow.getRowId());
		}

		return returnCode;
	}

	private ReturnCode checkNextRow(
			final byte[] row ) {
		if (Bytes.compareTo(
				row,
				nextRow) < 0) {
			return ReturnCode.SEEK_NEXT_USING_HINT;
		}
		else {
			return ReturnCode.INCLUDE;
		}
	}

	private void getNextRowKey(
			final byte[] row ) {
		nextRow = IndexUtils.getNextRowForSkip(
				row,
				bitPosition);
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final ByteBuffer buf = ByteBuffer.allocate(
				Integer.BYTES);
		buf.putInt(
				bitPosition);

		return buf.array();
	}

	public static FixedCardinalitySkippingFilter parseFrom(
			final byte[] bytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(
				bytes);
		final int bitpos = buf.getInt();

		return new FixedCardinalitySkippingFilter(
				bitpos);
	}

}
