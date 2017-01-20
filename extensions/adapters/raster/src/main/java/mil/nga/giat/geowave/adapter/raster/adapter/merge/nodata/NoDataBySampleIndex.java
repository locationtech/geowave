package mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class NoDataBySampleIndex implements
		NoDataMetadata
{
	private Set<SampleIndex> noDataIndexSet;

	protected NoDataBySampleIndex() {
		super();
	}

	public NoDataBySampleIndex(
			final Set<SampleIndex> noDataIndexSet ) {
		this.noDataIndexSet = noDataIndexSet;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(noDataIndexSet.size() * 12);
		for (final SampleIndex i : noDataIndexSet) {
			buf.putInt(i.getX());
			buf.putInt(i.getY());
			buf.putInt(i.getBand());
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = bytes.length / 12;
		noDataIndexSet = new HashSet<SampleIndex>(
				size);
		for (int i = 0; i < size; i++) {
			final int x = buf.getInt();
			final int y = buf.getInt();
			final int b = buf.getInt();
			noDataIndexSet.add(new SampleIndex(
					x,
					y,
					b));
		}
	}

	@Override
	public boolean isNoData(
			final SampleIndex index,
			final double sampleValue ) {
		return noDataIndexSet.contains(index);
	}

	@Override
	public Set<SampleIndex> getNoDataIndices() {
		return noDataIndexSet;
	}

}
