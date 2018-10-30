/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.index.numeric;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.index.FilterableConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/**
 * A class based on FilterableConstraints that uses numeric values and includes
 * a lower and upper range
 * 
 */

public class NumericQueryConstraint implements
		FilterableConstraints
{
	protected final String fieldName;
	protected Number lowerValue;
	protected Number upperValue;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public NumericQueryConstraint(
			final String fieldName,
			final Number lowerValue,
			final Number upperValue,
			final boolean inclusiveLow,
			final boolean inclusiveHigh ) {
		super();
		this.fieldName = fieldName;
		this.lowerValue = lowerValue;
		this.upperValue = upperValue;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
	}

	@Override
	public String getFieldName() {
		return fieldName;
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
	public QueryFilter getFilter() {
		return new NumberRangeFilter(
				fieldName,
				lowerValue,
				upperValue,
				inclusiveLow,
				inclusiveHigh);
	}

	public QueryRanges getQueryRanges() {
		return new QueryRanges(
				new ByteArrayRange(
						new ByteArray(
								NumericFieldIndexStrategy.toIndexByte(lowerValue.doubleValue())),
						new ByteArray(
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
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldName.equals(fieldName)) {
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
					fieldName,
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
		if ((other instanceof NumericQueryConstraint) && ((NumericQueryConstraint) other).fieldName.equals(fieldName)) {
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
					fieldName,
					newMin,
					newMax,
					newIncLow,
					newIncHigh);
		}
		return this;
	}
}
