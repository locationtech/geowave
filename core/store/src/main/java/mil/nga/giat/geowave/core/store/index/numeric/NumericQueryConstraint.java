package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public class NumericQueryConstraint implements
		FilterableConstraints
{
	protected final ByteArrayId fieldId;
	protected Number lowerValue;
	protected Number upperValue;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public NumericQueryConstraint(
			final ByteArrayId fieldId,
			final Number lowerValue,
			final Number upperValue,
			final boolean inclusiveLow,
			final boolean inclusiveHigh ) {
		super();
		this.fieldId = fieldId;
		this.lowerValue = lowerValue;
		this.upperValue = upperValue;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public int getDimensionCount() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new NumberRangeFilter(
				fieldId,
				lowerValue,
				upperValue,
				inclusiveLow,
				inclusiveHigh);
	}

	public QueryRanges getQueryRanges() {
		return new QueryRanges(
				new ByteArrayRange(
						new ByteArrayId(
								NumericFieldIndexStrategy.toIndexByte(lowerValue.doubleValue())),
						new ByteArrayId(
								NumericFieldIndexStrategy.toIndexByte(upperValue.doubleValue()))));
	}

	@Override
	public FilterableConstraints intersect(
			final FilterableConstraints other ) {
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldId.equals(fieldId)) {
			final NumericQueryConstraint otherNumeric = ((NumericQueryConstraint) other);

			final boolean lowEquals = lowerValue.equals(otherNumeric.lowerValue);
			final boolean upperEquals = upperValue.equals(otherNumeric.upperValue);
			final boolean replaceMin = (lowerValue.doubleValue() < otherNumeric.lowerValue.doubleValue());
			final boolean replaceMax = (upperValue.doubleValue() > otherNumeric.upperValue.doubleValue());
			return new NumericQueryConstraint(
					fieldId,
					Math.max(
							lowerValue.doubleValue(),
							otherNumeric.lowerValue.doubleValue()),
					Math.min(
							upperValue.doubleValue(),
							otherNumeric.upperValue.doubleValue()),
					lowEquals ? otherNumeric.inclusiveLow & inclusiveLow : (replaceMin ? otherNumeric.inclusiveLow
							: inclusiveLow),
					upperEquals ? otherNumeric.inclusiveHigh & inclusiveHigh : (replaceMax ? otherNumeric.inclusiveHigh
							: inclusiveHigh));
		}
		return this;
	}

	@Override
	public FilterableConstraints union(
			final FilterableConstraints other ) {
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldId.equals(fieldId)) {
			final NumericQueryConstraint otherNumeric = ((NumericQueryConstraint) other);

			final boolean lowEquals = lowerValue.equals(otherNumeric.lowerValue);
			final boolean upperEquals = upperValue.equals(otherNumeric.upperValue);
			final boolean replaceMin = (lowerValue.doubleValue() > otherNumeric.lowerValue.doubleValue());
			final boolean replaceMax = (upperValue.doubleValue() < otherNumeric.upperValue.doubleValue());
			return new NumericQueryConstraint(
					fieldId,
					Math.min(
							lowerValue.doubleValue(),
							otherNumeric.lowerValue.doubleValue()),
					Math.max(
							upperValue.doubleValue(),
							otherNumeric.upperValue.doubleValue()),
					lowEquals ? otherNumeric.inclusiveLow | inclusiveLow : (replaceMin ? otherNumeric.inclusiveLow
							: inclusiveLow),
					upperEquals ? otherNumeric.inclusiveHigh | inclusiveHigh : (replaceMax ? otherNumeric.inclusiveHigh
							: inclusiveHigh));
		}
		return this;
	}
}
