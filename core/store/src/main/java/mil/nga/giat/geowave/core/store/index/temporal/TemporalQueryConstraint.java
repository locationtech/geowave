package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

/**
 * A class based on FilterableConstraints that uses temporal values and includes
 * a start date and end date
 * 
 * @author geowave
 *
 */

public class TemporalQueryConstraint implements
		FilterableConstraints
{
	protected final ByteArrayId fieldId;
	protected final Date start;
	protected final Date end;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public TemporalQueryConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end ) {
		this(
				fieldId,
				start,
				end,
				false,
				false);
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	public TemporalQueryConstraint(
			final ByteArrayId fieldId,
			final Date start,
			final Date end,
			boolean inclusiveLow,
			boolean inclusiveHigh ) {
		super();
		this.fieldId = fieldId;
		this.start = start;
		this.end = end;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
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
		return new DateRangeFilter(
				fieldId,
				start,
				end,
				inclusiveLow,
				inclusiveHigh);
	}

	/**
	 * Creates a collection of range values based on start and end times for
	 * this constraint
	 * 
	 * @return
	 */

	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(start)),
				new ByteArrayId(
						TemporalIndexStrategy.toIndexByte(end))));
	}

	/**
	 * 
	 * Returns an FilterableConstraints object that is the intersection of the
	 * start and end times of this object and object passed in.
	 * <p>
	 * This method returns an object with the latest start and earliest end of
	 * the two objects
	 *
	 * @param otherConstraint
	 *            object whose constraints are 'intersected' with existing
	 *            constraints
	 * @return new {@link FilterableConstraints}
	 */

	@Override
	public FilterableConstraints intersect(
			FilterableConstraints constraints ) {
		if (constraints instanceof TemporalQueryConstraint) {
			TemporalQueryConstraint filterConstraints = (TemporalQueryConstraint) constraints;
			if (fieldId.equals(filterConstraints.fieldId)) {
				Date newStart = start.compareTo(filterConstraints.start) < 0 ? filterConstraints.start : start;
				Date newEnd = end.compareTo(filterConstraints.end) > 0 ? filterConstraints.end : end;
				final boolean lowEquals = start.equals(filterConstraints.start);
				final boolean upperEquals = end.equals(filterConstraints.end);
				final boolean replaceMin = start.compareTo(filterConstraints.start) < 0;
				final boolean replaceMax = end.compareTo(filterConstraints.end) > 0;

				boolean newInclusiveLow = lowEquals ? filterConstraints.inclusiveLow & inclusiveLow
						: (replaceMin ? filterConstraints.inclusiveLow : inclusiveLow);
				boolean newInclusiveHigh = upperEquals ? filterConstraints.inclusiveHigh & inclusiveHigh
						: (replaceMax ? filterConstraints.inclusiveHigh : inclusiveHigh);

				return new TemporalQueryConstraint(
						fieldId,
						newStart,
						newEnd,
						newInclusiveLow,
						newInclusiveHigh);
			}
		}
		return this;
	}

	/**
	 * 
	 * Returns an FilterableConstraints object that is the union of the start
	 * and end times of this object and object passed in.
	 * <p>
	 * This method returns an object with the earliest start and latest end time
	 * of the two objects
	 *
	 * @param otherConstraint
	 *            object whose constraints are 'unioned' with existing
	 *            constraints
	 * @return new {@link FilterableConstraints}
	 */

	@Override
	public FilterableConstraints union(
			FilterableConstraints constraints ) {
		if (constraints instanceof TemporalQueryConstraint) {
			TemporalQueryConstraint filterConstraints = (TemporalQueryConstraint) constraints;
			if (fieldId.equals(filterConstraints.fieldId)) {
				Date newStart = start.compareTo(filterConstraints.start) > 0 ? filterConstraints.start : start;
				Date newEnd = end.compareTo(filterConstraints.end) < 0 ? filterConstraints.end : end;
				final boolean lowEquals = start.equals(filterConstraints.start);
				final boolean upperEquals = end.equals(filterConstraints.end);
				final boolean replaceMin = start.compareTo(filterConstraints.start) > 0;
				final boolean replaceMax = end.compareTo(filterConstraints.end) < 0;

				boolean newInclusiveLow = lowEquals ? filterConstraints.inclusiveLow | inclusiveLow
						: (replaceMin ? filterConstraints.inclusiveLow : inclusiveLow);
				boolean newInclusiveHigh = upperEquals ? filterConstraints.inclusiveHigh | inclusiveHigh
						: (replaceMax ? filterConstraints.inclusiveHigh : inclusiveHigh);

				return new TemporalQueryConstraint(
						fieldId,
						newStart,
						newEnd,
						newInclusiveLow,
						newInclusiveHigh);
			}
		}
		return this;
	}
}
