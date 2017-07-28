/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

/**
 * A class based on FilterableConstraints that uses numeric values and includes
 * a lower and upper range
 * 
 */

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

	public double getMinValue() {
		return lowerValue.doubleValue();
	}

	public double getMaxValue() {
		return upperValue.doubleValue();
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

	/**
	 * 
	 * Returns an FilterableConstraints object that is the intersection of the
	 * numeric bounds of this object and object passed in.
	 * <p>
	 * This method returns an object with the highest min and lowest max of the
	 * two objects
	 *
	 * @param otherConstraint
	 *            object whose constraints are 'intersected' with existing
	 *            constraints
	 * @return new {@link FilterableConstraints}
	 */

	@Override
	public FilterableConstraints intersect(
			final FilterableConstraints other ) {
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldId.equals(fieldId)) {
			final NumericQueryConstraint otherNumeric = ((NumericQueryConstraint) other);

			final boolean lowEquals = lowerValue.equals(otherNumeric.lowerValue);
			final boolean upperEquals = upperValue.equals(otherNumeric.upperValue);
			final boolean replaceMin = (lowerValue.doubleValue() < otherNumeric.lowerValue.doubleValue());
			final boolean replaceMax = (upperValue.doubleValue() > otherNumeric.upperValue.doubleValue());
			double newMin = Math.max(
					this.lowerValue.doubleValue(),
					otherNumeric.lowerValue.doubleValue());
			double newMax = Math.min(
					this.upperValue.doubleValue(),
					otherNumeric.upperValue.doubleValue());
			boolean newIncLow = lowEquals ? (otherNumeric.inclusiveLow & inclusiveLow)
					: (replaceMin ? otherNumeric.inclusiveLow : inclusiveLow);
			boolean newIncHigh = upperEquals ? (otherNumeric.inclusiveHigh & inclusiveHigh)
					: (replaceMax ? otherNumeric.inclusiveHigh : inclusiveHigh);

			return new NumericQueryConstraint(
					fieldId,
					newMin,
					newMax,
					newIncLow,
					newIncHigh);

		}
		return this;
	}

	/**
	 * 
	 * Returns an FilterableConstraints object that is the union of the numeric
	 * bounds of this object and object passed in.
	 * <p>
	 * This method returns an object with the lowest min and highest max of the
	 * two objects
	 *
	 * @param otherConstraint
	 *            object whose constraints are 'unioned' with existing
	 *            constraints
	 * @return new {@link FilterableConstraints}
	 */
	@Override
	public FilterableConstraints union(
			final FilterableConstraints other ) {
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldId.equals(fieldId)) {
			final NumericQueryConstraint otherNumeric = ((NumericQueryConstraint) other);

			final boolean lowEquals = lowerValue.equals(otherNumeric.lowerValue);
			final boolean upperEquals = upperValue.equals(otherNumeric.upperValue);
			final boolean replaceMin = (lowerValue.doubleValue() > otherNumeric.lowerValue.doubleValue());
			final boolean replaceMax = (upperValue.doubleValue() < otherNumeric.upperValue.doubleValue());
			double newMin = Math.min(
					this.lowerValue.doubleValue(),
					otherNumeric.lowerValue.doubleValue());
			double newMax = Math.max(
					this.upperValue.doubleValue(),
					otherNumeric.upperValue.doubleValue());

			boolean newIncLow = lowEquals ? (otherNumeric.inclusiveLow | inclusiveLow)
					: (replaceMin ? otherNumeric.inclusiveLow : inclusiveLow);
			boolean newIncHigh = upperEquals ? (otherNumeric.inclusiveHigh | inclusiveHigh)
					: (replaceMax ? otherNumeric.inclusiveHigh : inclusiveHigh);

			return new NumericQueryConstraint(
					fieldId,
					newMin,
					newMax,
					newIncLow,
					newIncHigh);
		}
		return this;
	}
}
