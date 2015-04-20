package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

/**
 * This class is a concrete implementation of a common index model. Data
 * adapters will map their adapter specific fields to these fields that are
 * common for a given index. This way distributable filters will not need to
 * handle any adapter-specific transformation, but can use the common index
 * fields.
 * 
 */
public class BasicIndexModel implements
		CommonIndexModel
{
	private DimensionField<?>[] dimensions;
	// the first dimension of a particular field ID will be the persistence
	// model used
	private Map<ByteArrayId, DimensionField<?>> fieldIdToPeristenceMap;

	protected BasicIndexModel() {}

	public BasicIndexModel(
			final DimensionField<?>[] dimensions ) {
		init(dimensions);
	}

	private void init(
			final DimensionField<?>[] dimensions ) {
		this.dimensions = dimensions;
		fieldIdToPeristenceMap = new HashMap<ByteArrayId, DimensionField<?>>();
		for (final DimensionField<?> d : dimensions) {
			if (!fieldIdToPeristenceMap.containsKey(d.getFieldId())) {
				fieldIdToPeristenceMap.put(
						d.getFieldId(),
						d);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<Object, CommonIndexValue> getWriter(
			final ByteArrayId fieldId ) {
		final DimensionField<?> dimension = fieldIdToPeristenceMap.get(fieldId);
		if (dimension != null) {
			return (FieldWriter<Object, CommonIndexValue>) dimension.getWriter();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<CommonIndexValue> getReader(
			final ByteArrayId fieldId ) {
		final DimensionField<?> dimension = fieldIdToPeristenceMap.get(fieldId);
		if (dimension != null) {
			return (FieldReader<CommonIndexValue>) dimension.getReader();
		}
		return null;
	}

	@Override
	public DimensionField<?>[] getDimensions() {
		return dimensions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + Arrays.hashCode(dimensions);
		return result;
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
		final BasicIndexModel other = (BasicIndexModel) obj;
		return Arrays.equals(
				dimensions,
				other.dimensions);
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 4;
		final List<byte[]> dimensionBinaries = new ArrayList<byte[]>(
				dimensions.length);
		for (final DimensionField<?> dimension : dimensions) {
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			byteBufferLength += (4 + dimensionBinary.length);
			dimensionBinaries.add(dimensionBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(dimensions.length);
		for (final byte[] dimensionBinary : dimensionBinaries) {
			buf.putInt(dimensionBinary.length);
			buf.put(dimensionBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numDimensions = buf.getInt();
		dimensions = new DimensionField[numDimensions];
		for (int i = 0; i < numDimensions; i++) {
			final byte[] dim = new byte[buf.getInt()];
			buf.get(dim);
			dimensions[i] = PersistenceUtils.fromBinary(
					dim,
					DimensionField.class);
		}
		init(dimensions);
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}
}
