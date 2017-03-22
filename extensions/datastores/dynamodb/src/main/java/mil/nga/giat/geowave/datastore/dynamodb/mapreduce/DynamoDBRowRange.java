package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;

import org.apache.hadoop.io.WritableUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class DynamoDBRowRange implements
		GeoWaveRowRange
{
	private ByteArrayRange range;
	private boolean infiniteStartKey = false;
	private boolean infiniteEndKey = false;

	public DynamoDBRowRange(
			final ByteArrayRange range ) {
		this.range = range;
	}

	public DynamoDBRowRange() {
		range = null;
		infiniteStartKey = true;
		infiniteEndKey = true;
	}

	public ByteArrayRange getRange() {
		return range;
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {

		byte[] startBytes = new byte[0];
		byte[] endBytes = new byte[0];

		if (!infiniteStartKey) {
			startBytes = range.getStart().getBytes();
		}
		if (!infiniteEndKey) {
			endBytes = range.getEnd().getBytes();
		}

		out.writeBoolean(infiniteStartKey);
		out.writeBoolean(infiniteEndKey);

		WritableUtils.writeVInt(
				out,
				startBytes.length);
		WritableUtils.writeVInt(
				out,
				endBytes.length);

		out.write(startBytes);
		out.write(endBytes);

	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {

		infiniteStartKey = in.readBoolean();
		infiniteEndKey = in.readBoolean();

		final int startKeyLen = WritableUtils.readVInt(in);
		final int endKeyLen = WritableUtils.readVInt(in);

		byte[] startBytes = null;
		byte[] endBytes = null;

		if (!infiniteStartKey) {
			startBytes = new byte[startKeyLen];
			in.readFully(startBytes);
		}
		if (!infiniteEndKey) {
			endBytes = new byte[endKeyLen];
			in.readFully(endBytes);
		}

		range = new ByteArrayRange(
				new ByteArrayId(
						startBytes),
				new ByteArrayId(
						endBytes));

		if (range.getStart().compareTo(
				range.getEnd()) > 0) {
			throw new InvalidObjectException(
					"Start key must be less than end key in range (" + range.getStart().getHexString() + ", "
							+ range.getEnd().getHexString() + ")");
		}

	}

	@Override
	public byte[] getStartKey() {
		if (!infiniteStartKey) {
			return range.getStart().getBytes();
		}
		return null;
	}

	@Override
	public byte[] getEndKey() {
		if (!infiniteEndKey) {
			return range.getEnd().getBytes();
		}
		return null;
	}

	@Override
	public boolean isStartKeyInclusive() {
		return true;
	}

	@Override
	public boolean isEndKeyInclusive() {
		return false;
	}

	@Override
	public boolean isInfiniteStartKey() {
		return infiniteStartKey;
	}

	@Override
	public boolean isInfiniteStopKey() {
		return infiniteEndKey;
	}
}
