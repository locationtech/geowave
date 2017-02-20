package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class DataIdQueryFilter implements
		DistributableQueryFilter
{
	private List<ByteArrayId> dataIds;

	protected DataIdQueryFilter() {}

	public DataIdQueryFilter(
			final List<ByteArrayId> dataIds ) {
		this.dataIds = dataIds;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return dataIds.contains(persistenceEncoding.getDataId());
	}

	@Override
	public byte[] toBinary() {
		int size = 4;
		for (ByteArrayId id : dataIds) {
			size += (id.getBytes().length + 4);
		}
		final ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(dataIds.size());
		for (final ByteArrayId id : dataIds) {
			final byte[] idBytes = id.getBytes();
			buf.putInt(idBytes.length);
			buf.put(idBytes);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = buf.getInt();
		dataIds = new ArrayList<ByteArrayId>(
				size);
		for (int i = 0; i < size; i++) {
			final int bsize = buf.getInt();
			final byte[] dataIdBytes = new byte[bsize];
			buf.get(dataIdBytes);
			dataIds.add(new ByteArrayId(
					dataIdBytes));
		}
	}

}
