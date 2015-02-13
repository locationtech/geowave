package mil.nga.giat.geowave.store.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.filter.BasicQueryFilter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.log4j.Logger;

/**
 * The Basic Query class represent a hyper-cube query across all dimensions that
 * match the Constraints passed into the constructor
 */
public class BasicQuery implements
		DistributableQuery
{
	private final static Logger LOGGER = Logger.getLogger(BasicQuery.class);

	public static class ConstraintData
	{
		protected final NumericData range;
		protected final boolean isDefault;

		public ConstraintData(
				final NumericData range,
				final boolean isDefault ) {
			super();
			this.range = range;
			this.isDefault = isDefault;
		}
	}

	public static class Constraints
	{
		// these basic queries are tied to NumericDimensionDefinition types, not
		// ideal, but third-parties can and will nned to implement their own
		// queries if they implement their own dimension definitions
		protected final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition;

		public Constraints() {
			constraintsPerTypeOfDimensionDefinition = new LinkedHashMap<Class<? extends NumericDimensionDefinition>, ConstraintData>();
		}

		public Constraints(
				final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition ) {
			this.constraintsPerTypeOfDimensionDefinition = constraintsPerTypeOfDimensionDefinition;
		}

		public Constraints merge(
				Constraints constraints ) {
			this.constraintsPerTypeOfDimensionDefinition.putAll(constraints.constraintsPerTypeOfDimensionDefinition);
			return this;
		}

		public void addConstraint(
				final Class<? extends NumericDimensionDefinition> dimDefinition,
				final ConstraintData constraintData ) {
			constraintsPerTypeOfDimensionDefinition.put(
					dimDefinition,
					constraintData);
		}

		public boolean isEmpty() {
			return constraintsPerTypeOfDimensionDefinition.isEmpty();
		}

		public boolean matches(
				Constraints constraints ) {
			if (constraints.isEmpty() != this.isEmpty()) return false;
			for (final Map.Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> entry : constraintsPerTypeOfDimensionDefinition.entrySet()) {
				ConstraintData data = constraints.constraintsPerTypeOfDimensionDefinition.get(entry.getKey());
				if (!data.range.equals(entry.getValue().range)) return false;
			}
			return true;
		}

		public MultiDimensionalNumericData getIndexConstraints(
				final NumericIndexStrategy indexStrategy ) {
			if (constraintsPerTypeOfDimensionDefinition.isEmpty()) {
				return new BasicNumericDataset();
			}
			final NumericDimensionDefinition[] dimensionDefinitions = indexStrategy.getOrderedDimensionDefinitions();
			final NumericData[] dataPerDimension = new NumericData[dimensionDefinitions.length];
			// all or nothing...for now
			for (int d = 0; d < dimensionDefinitions.length; d++) {
				final ConstraintData dimConstraint = constraintsPerTypeOfDimensionDefinition.get(dimensionDefinitions[d].getClass());
				dataPerDimension[d] = (dimConstraint == null ? dimensionDefinitions[d].getFullRange() : dimConstraint.range);
			}
			return new BasicNumericDataset(
					dataPerDimension);
		}

		public boolean isSupported(
				final Index index ) {
			final DimensionField<? extends CommonIndexValue>[] fields = index.getIndexModel().getDimensions();
			final Set<Class<? extends NumericDimensionDefinition>> fieldTypeSet = new HashSet<Class<? extends NumericDimensionDefinition>>();
			// first create a set of the field's base definition types that are
			// within the index model
			for (final DimensionField<? extends CommonIndexValue> field : fields) {
				fieldTypeSet.add(field.getBaseDefinition().getClass());
			}
			// then ensure each of the definition types that is required by
			// these
			// constraints are in the index model
			for (final Map.Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> entry : constraintsPerTypeOfDimensionDefinition.entrySet()) {
				// defaults are not mandatory
				if (!fieldTypeSet.contains(entry.getKey()) && !entry.getValue().isDefault) {
					return false;
				}
			}
			return true;
		}
	}

	private Constraints constraints;

	protected BasicQuery() {}

	public BasicQuery(
			final Constraints constraints ) {
		this.constraints = constraints;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final DimensionField<?>[] dimensionFields = indexModel.getDimensions();
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		final NumericData[] orderedConstraintsPerDimension = new NumericData[dimensionFields.length];
		for (int d = 0; d < dimensionFields.length; d++) {
			final ConstraintData nd = constraints.constraintsPerTypeOfDimensionDefinition.get(dimensionFields[d].getBaseDefinition().getClass());
			if (nd == null) {
				orderedConstraintsPerDimension[d] = dimensionFields[d].getBaseDefinition().getFullRange();
			}
			else {
				orderedConstraintsPerDimension[d] = constraints.constraintsPerTypeOfDimensionDefinition.get(dimensionFields[d].getBaseDefinition().getClass()).range;
			}
		}
		final QueryFilter queryFilter = createQueryFilter(
				new BasicNumericDataset(
						orderedConstraintsPerDimension),
				dimensionFields);
		if (queryFilter != null) {
			filters.add(queryFilter);
		}
		return filters;
	}

	protected QueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields ) {
		return new BasicQueryFilter(
				constraints,
				dimensionFields);
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		return constraints.isSupported(index);
	}

	@Override
	public MultiDimensionalNumericData getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return constraints.getIndexConstraints(indexStrategy);
	}

	@Override
	public byte[] toBinary() {
		final List<byte[]> bytes = new ArrayList<byte[]>(
				constraints.constraintsPerTypeOfDimensionDefinition.size());
		int totalBytes = 4;
		for (final Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> c : constraints.constraintsPerTypeOfDimensionDefinition.entrySet()) {
			final byte[] className = StringUtils.stringToBinary(c.getKey().getName());
			final double min = c.getValue().range.getMin();
			final double max = c.getValue().range.getMax();
			final int entryLength = className.length + 22;
			final short isDefault = (short) (c.getValue().isDefault ? 1 : 0);
			final ByteBuffer entryBuf = ByteBuffer.allocate(entryLength);
			entryBuf.putInt(className.length);
			entryBuf.put(className);
			entryBuf.putDouble(min);
			entryBuf.putDouble(max);
			entryBuf.putShort(isDefault);
			bytes.add(entryBuf.array());
			totalBytes += entryLength;
		}

		final ByteBuffer buf = ByteBuffer.allocate(totalBytes);
		buf.putInt(bytes.size());
		for (final byte[] entryBytes : bytes) {
			buf.put(entryBytes);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numEntries = buf.getInt();
		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition = new LinkedHashMap<Class<? extends NumericDimensionDefinition>, ConstraintData>(
				numEntries);
		for (int i = 0; i < numEntries; i++) {
			final int classNameLength = buf.getInt();
			final byte[] className = new byte[classNameLength];
			buf.get(className);
			final double min = buf.getDouble();
			final double max = buf.getDouble();
			final boolean isDefault = buf.getShort() > 0;
			final String classNameStr = StringUtils.stringFromBinary(className);
			try {
				final Class<? extends NumericDimensionDefinition> cls = (Class<? extends NumericDimensionDefinition>) Class.forName(classNameStr);
				constraintsPerTypeOfDimensionDefinition.put(
						cls,
						new ConstraintData(
								new NumericRange(
										min,
										max),
								isDefault));
			}
			catch (final ClassNotFoundException e) {
				LOGGER.warn(
						"Cannot find dimension definition class: " + classNameStr,
						e);
			}
		}
		constraints = new Constraints(
				constraintsPerTypeOfDimensionDefinition);
	}
}
