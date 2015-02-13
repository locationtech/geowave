package mil.nga.giat.geowave.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.store.query.BasicQuery.Constraints;

abstract public class NumericRangeDataStatistics<T> extends
		AbstractDataStatistics<T>
{

	private double min = Double.MAX_VALUE;
	private double max = 0;

	protected NumericRangeDataStatistics() {
		super();
	}

	public NumericRangeDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsIds ) {
		super(
				dataAdapterId,
				statisticsIds);
	}

	public boolean isSet() {
		if ((min == Double.MAX_VALUE) || (max == 0)) {
			return false;
		}
		return true;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getRange() {
		return max - min;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(16);
		buffer.putDouble(min);
		buffer.putDouble(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		min = buffer.getDouble();
		max = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final NumericRange range = getRange(entry);
		if (range != null) {
			min = Math.min(
					min,
					range.getMin());
			max = Math.max(
					max,
					range.getMax());
		}
	}

	public Constraints getConstraints() {
		final Constraints constraints = new Constraints();
		constraints.addConstraint(
				TimeDefinition.class,
				new ConstraintData(
						new NumericRange(
								min,
								max),
						true));
		return constraints;
	}

	abstract protected NumericRange getRange(
			final T entry );

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof NumericRangeDataStatistics)) {
			final NumericRangeDataStatistics<T> stats = (NumericRangeDataStatistics<T>) statistics;
			if (stats.isSet()) {
				min = Math.min(
						min,
						stats.getMin());
				max = Math.max(
						max,
						stats.getMax());
			}
		}
	}
}
