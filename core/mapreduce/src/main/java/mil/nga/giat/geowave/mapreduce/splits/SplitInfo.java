package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class SplitInfo
{
	private PrimaryIndex index;
	private List<RangeLocationPair> rangeLocationPairs;
	private boolean mixedVisibility = true;

	protected SplitInfo() {}

	public SplitInfo(
			final PrimaryIndex index ) {
		this.index = index;
		rangeLocationPairs = new ArrayList<RangeLocationPair>();
	}

	public SplitInfo(
			final PrimaryIndex index,
			final List<RangeLocationPair> rangeLocationPairs ) {
		super();
		this.index = index;
		this.rangeLocationPairs = rangeLocationPairs;
	}

	public boolean isMixedVisibility() {
		return mixedVisibility;
	}

	public void setMixedVisibility(
			final boolean mixedVisibility ) {
		this.mixedVisibility = mixedVisibility;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	public List<RangeLocationPair> getRangeLocationPairs() {
		return rangeLocationPairs;
	}

	public void readFields(
			final DataInput in )
			throws IOException {
		final int indexLength = in.readInt();
		final byte[] indexBytes = new byte[indexLength];
		in.readFully(indexBytes);
		final PrimaryIndex index = (PrimaryIndex) PersistenceUtils.fromBinary(indexBytes);
		final int numRanges = in.readInt();
		final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>(
				numRanges);

		for (int j = 0; j < numRanges; j++) {
			try {
				final RangeLocationPair range = new RangeLocationPair();
				range.readFields(in);
				rangeList.add(range);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						"Unable to instantiate range",
						e);
			}
		}
		this.index = index;
		rangeLocationPairs = rangeList;
		mixedVisibility = in.readBoolean();
	}

	public void write(
			final DataOutput out )
			throws IOException {
		final byte[] indexBytes = PersistenceUtils.toBinary(index);
		out.writeInt(indexBytes.length);
		out.write(indexBytes);
		out.writeInt(rangeLocationPairs.size());
		for (final RangeLocationPair r : rangeLocationPairs) {
			r.write(out);
		}
		out.writeBoolean(mixedVisibility);
	}
}
