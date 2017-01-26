package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

/**
 * This class fully describes everything necessary to index data within GeoWave
 * using secondary indexing. <br>
 * The key components are the indexing strategy and the common index model. <br>
 * <br>
 * Attributes for SecondaryIndex include:<br>
 * indexStrategy = array of fieldIndexStrategy (numeric, temporal or text)<br>
 * fieldId<br>
 * associatedStatistics <br>
 * secondaryIndexType - (join, full, partial)<br>
 * secondaryIndexId - <br>
 * partialFieldIds - list of fields that are part of the ...<br>
 */

public class SecondaryIndex<T> implements
		Index<FilterableConstraints, List<FieldInfo<?>>>
{
	private static final String TABLE_PREFIX = "GEOWAVE_2ND_IDX_";
	private FieldIndexStrategy<?, ?> indexStrategy;
	private ByteArrayId fieldId;
	private List<DataStatistics<T>> associatedStatistics;
	private SecondaryIndexType secondaryIndexType;
	private ByteArrayId secondaryIndexId;
	private List<ByteArrayId> partialFieldIds;

	protected SecondaryIndex() {}

	public SecondaryIndex(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final ByteArrayId fieldId,
			final List<DataStatistics<T>> associatedStatistics,
			final SecondaryIndexType secondaryIndexType ) {
		this(
				indexStrategy,
				fieldId,
				associatedStatistics,
				secondaryIndexType,
				Collections.<ByteArrayId> emptyList());
	}

	public SecondaryIndex(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final ByteArrayId fieldId,
			final List<DataStatistics<T>> associatedStatistics,
			final SecondaryIndexType secondaryIndexType,
			final List<ByteArrayId> partialFieldIds ) {
		super();
		this.indexStrategy = indexStrategy;
		this.fieldId = fieldId;
		this.associatedStatistics = associatedStatistics;
		this.secondaryIndexType = secondaryIndexType;
		this.secondaryIndexId = new ByteArrayId(
				TABLE_PREFIX + indexStrategy.getId() + "_" + secondaryIndexType.getValue());
		this.partialFieldIds = partialFieldIds;
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	@Override
	public FieldIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public ByteArrayId getFieldId() {
		return fieldId;
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

	public List<ByteArrayId> getPartialFieldIds() {
		return partialFieldIds;
	}

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	/**
	 * Compare this object to the one passed as parameter to see if same object,
	 * same class and that id is the same.
	 */

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
		final byte[] fieldIdBinary = fieldId.getBytes();
		final byte[] secondaryIndexTypeBinary = StringUtils.stringToBinary(secondaryIndexType.getValue());
		final List<Persistable> persistables = new ArrayList<Persistable>();
		for (DataStatistics<T> dataStatistics : associatedStatistics) {
			persistables.add(dataStatistics);
		}
		final byte[] persistablesBinary = PersistenceUtils.toBinary(persistables);
		final boolean handlePartials = (partialFieldIds != null && !partialFieldIds.isEmpty());
		int partialsLength = 0;
		byte[] partialsBinary = null;
		if (handlePartials) {
			int totalLength = 0;
			for (final ByteArrayId partialFieldId : partialFieldIds) {
				totalLength += partialFieldId.getBytes().length;
			}
			final ByteBuffer allPartials = ByteBuffer.allocate(totalLength + (partialFieldIds.size() * 4));
			for (final ByteArrayId partialFieldId : partialFieldIds) {
				allPartials.putInt(partialFieldId.getBytes().length);
				allPartials.put(partialFieldId.getBytes());
			}
			partialsLength = allPartials.array().length;
			partialsBinary = allPartials.array();
		}
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + fieldIdBinary.length
				+ secondaryIndexTypeBinary.length + 20 + persistablesBinary.length + partialsLength
				+ (partialsLength > 0 ? 4 : 0));
		buf.putInt(indexStrategyBinary.length);
		buf.putInt(fieldIdBinary.length);
		buf.putInt(secondaryIndexTypeBinary.length);
		buf.putInt(persistablesBinary.length);
		buf.putInt(handlePartials ? partialFieldIds.size() : 0);
		buf.put(indexStrategyBinary);
		buf.put(fieldIdBinary);
		buf.put(secondaryIndexTypeBinary);
		buf.put(persistablesBinary);
		if (handlePartials) {
			buf.putInt(partialsLength);
			buf.put(partialsBinary);
		}
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
		final int persistablesBinaryLength = buf.getInt();
		final int numPartials = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		final byte[] fieldIdBinary = new byte[fieldIdLength];
		final byte[] secondaryIndexTypeBinary = new byte[secondaryIndexTypeLength];
		buf.get(indexStrategyBinary);
		buf.get(fieldIdBinary);
		buf.get(secondaryIndexTypeBinary);

		indexStrategy = PersistenceUtils.fromBinary(
				indexStrategyBinary,
				FieldIndexStrategy.class);

		fieldId = new ByteArrayId(
				fieldIdBinary);

		secondaryIndexType = SecondaryIndexType.valueOf(StringUtils.stringFromBinary(secondaryIndexTypeBinary));

		final byte[] persistablesBinary = new byte[persistablesBinaryLength];
		buf.get(persistablesBinary);
		final List<Persistable> persistables = PersistenceUtils.fromBinary(persistablesBinary);
		for (final Persistable persistable : persistables) {
			associatedStatistics.add((DataStatistics<T>) persistable);
		}
		secondaryIndexId = new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId() + "_" + secondaryIndexType.getValue()));

		if (numPartials > 0) {
			partialFieldIds = new ArrayList<>();
			final int partialsLength = buf.getInt();
			final byte[] partialsBinary = new byte[partialsLength];
			final ByteBuffer partialsBB = ByteBuffer.wrap(partialsBinary);
			for (int i = 0; i < numPartials; i++) {
				final int currPartialLength = partialsBB.getInt();
				final byte[] currPartialBinary = new byte[currPartialLength];
				partialFieldIds.add(new ByteArrayId(
						currPartialBinary));
			}
		}
	}

}