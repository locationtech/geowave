package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.PersistenceUtils;

/**
 * This class wraps a list of distributable filters into a single distributable
 * filter such that the list is persisted in its original order and if any one
 * filter fails this class will fail acceptance.
 * 
 */
public class DistributableFilterList extends
		FilterList<DistributableQueryFilter> implements
		DistributableQueryFilter
{
	protected DistributableFilterList() {
		super();
	}

	public DistributableFilterList(
			final List<DistributableQueryFilter> filters ) {
		super(
				filters);
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 4;
		final List<byte[]> filterBinaries = new ArrayList<byte[]>(
				filters.size());
		for (final DistributableQueryFilter filter : filters) {
			final byte[] filterBinary = PersistenceUtils.toBinary(filter);
			byteBufferLength += (4 + filterBinary.length);
			filterBinaries.add(filterBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(filters.size());
		for (final byte[] filterBinary : filterBinaries) {
			buf.putInt(filterBinary.length);
			buf.put(filterBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numFilters = buf.getInt();
		filters = new ArrayList<DistributableQueryFilter>(
				numFilters);
		for (int i = 0; i < numFilters; i++) {
			final byte[] filter = new byte[buf.getInt()];
			buf.get(filter);
			filters.add(PersistenceUtils.fromBinary(
					filter,
					DistributableQueryFilter.class));
		}
	}

}
