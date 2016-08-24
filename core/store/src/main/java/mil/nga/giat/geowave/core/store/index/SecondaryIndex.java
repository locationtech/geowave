package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

/**
 * This class fully describes everything necessary to index data within GeoWave.
 * The key components are the indexing strategy and the common index model.
 */
public class SecondaryIndex<T> implements
		Index<FilterableConstraints, List<FieldInfo<?>>>
{
	private FieldIndexStrategy<?, ?> indexStrategy;
	private ByteArrayId[] fieldIDs;
	private final List<DataStatistics<T>> associatedStatistics;
	private SecondaryIndexType secondaryIndexType;
	private ByteArrayId secondaryIndexId;

	public SecondaryIndex(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final ByteArrayId[] fieldIDs,
			final List<DataStatistics<T>> associatedStatistics,
			final SecondaryIndexType secondaryIndexType ) {
		super();
		this.indexStrategy = indexStrategy;
		this.fieldIDs = fieldIDs;
		this.associatedStatistics = associatedStatistics;
		this.secondaryIndexType = secondaryIndexType;
		this.secondaryIndexId = new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId() + "_" + secondaryIndexType.getValue()));
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	@Override
	public FieldIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public ByteArrayId[] getFieldIDs() {
		return fieldIDs;
	}

	@Override
	public ByteArrayId getId() {
		return secondaryIndexId;
	}

	public List<DataStatistics<T>> getAssociatedStatistics() {
		return associatedStatistics;
	}

	public SecondaryIndexType getSecondaryIndexType() {
		return secondaryIndexType;
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
		final SecondaryIndex<?> other = (SecondaryIndex<?>) obj;
		return getId().equals(
				other.getId());
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
		final byte[] fieldIdBinary = ByteArrayId.toBytes(fieldIDs);
		final byte[] secondaryIndexTypeBinary = StringUtils.stringToBinary(secondaryIndexType.getValue());
		final List<Persistable> persistables = new ArrayList<Persistable>();
		for (DataStatistics<T> dataStatistics : associatedStatistics) {
			persistables.add(dataStatistics);
		}
		final byte[] persistablesBinary = PersistenceUtils.toBinary(persistables);
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + fieldIdBinary.length
				+ secondaryIndexTypeBinary.length + 12 + persistablesBinary.length);
		buf.putInt(indexStrategyBinary.length);
		buf.putInt(fieldIdBinary.length);
		buf.putInt(secondaryIndexTypeBinary.length);
		buf.put(indexStrategyBinary);
		buf.put(fieldIdBinary);
		buf.put(secondaryIndexTypeBinary);
		buf.put(persistablesBinary);
		return buf.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int indexStrategyLength = buf.getInt();
		final int fieldIdLength = buf.getInt();
		final int secondaryIndexTypeLength = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		final byte[] fieldIdBinary = new byte[fieldIdLength];
		final byte[] secondaryIndexTypeBinary = new byte[secondaryIndexTypeLength];
		buf.get(indexStrategyBinary);
		buf.get(fieldIdBinary);
		buf.get(secondaryIndexTypeBinary);

		indexStrategy = PersistenceUtils.fromBinary(
				indexStrategyBinary,
				FieldIndexStrategy.class);

		fieldIDs = ByteArrayId.fromBytes(fieldIdBinary);

		secondaryIndexType = SecondaryIndexType.valueOf(StringUtils.stringFromBinary(secondaryIndexTypeBinary));

		final byte[] persistablesBinary = new byte[bytes.length - indexStrategyLength - fieldIdLength
				- secondaryIndexTypeLength - 12];
		buf.get(persistablesBinary);
		final List<Persistable> persistables = PersistenceUtils.fromBinary(persistablesBinary);
		for (final Persistable persistable : persistables) {
			associatedStatistics.add((DataStatistics<T>) persistable);
		}
		secondaryIndexId = new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId() + "_" + secondaryIndexType.getValue()));
	}

}