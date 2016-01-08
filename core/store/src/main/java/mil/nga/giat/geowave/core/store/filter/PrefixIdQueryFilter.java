package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class PrefixIdQueryFilter implements
		DistributableQueryFilter
{
	private ByteArrayId rowPrefix;

	protected PrefixIdQueryFilter() {}

	public PrefixIdQueryFilter(
			final ByteArrayId rowPrefix ) {
		this.rowPrefix = rowPrefix;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		ByteArrayId rowId = persistenceEncoding.getIndexInsertionId();
		return (Arrays.equals(
				rowPrefix.getBytes(),
				Arrays.copyOf(
						rowId.getBytes(),
						rowId.getBytes().length)));
	}

	@Override
	public byte[] toBinary() {
		return rowPrefix.getBytes();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		rowPrefix = new ByteArrayId(
				bytes);
	}

}
