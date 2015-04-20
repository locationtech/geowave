package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;

/**
 * This class fully describes everything necessary to index data within GeoWave.
 * The key components are the indexing strategy and the common index model.
 */
public class Index implements
		Persistable
{
	protected NumericIndexStrategy indexStrategy;
	protected CommonIndexModel indexModel;

	protected Index() {}

	public Index(
			final NumericIndexStrategy indexStrategy,
			final CommonIndexModel indexModel ) {
		this.indexStrategy = indexStrategy;
		this.indexModel = indexModel;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public CommonIndexModel getIndexModel() {
		return indexModel;
	}

	public ByteArrayId getId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId() + "_" + indexModel.getId()));
	}

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Index other = (Index) obj;
		return getId().equals(
				other.getId());
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
		final byte[] indexModelBinary = PersistenceUtils.toBinary(indexModel);
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + indexModelBinary.length + 4);
		buf.putInt(indexStrategyBinary.length);
		buf.put(indexStrategyBinary);
		buf.put(indexModelBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int indexStrategyLength = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		buf.get(indexStrategyBinary);

		indexStrategy = PersistenceUtils.fromBinary(
				indexStrategyBinary,
				NumericIndexStrategy.class);

		final byte[] indexModelBinary = new byte[bytes.length - indexStrategyLength - 4];
		buf.get(indexModelBinary);
		indexModel = PersistenceUtils.fromBinary(
				indexModelBinary,
				CommonIndexModel.class);
	}
}
