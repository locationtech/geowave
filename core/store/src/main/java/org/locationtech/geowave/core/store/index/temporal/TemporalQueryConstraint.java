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
package org.locationtech.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.index.FilterableConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/**
 * A class based on FilterableConstraints that uses temporal values and includes
 * a start date and end date
 * 
 */

public class TemporalQueryConstraint implements
		FilterableConstraints
{
	protected final String fieldName;
	protected final Date start;
	protected final Date end;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public TemporalQueryConstraint(
			final String fieldName,
			final Date start,
			final Date end ) {
		this(
				fieldName,
				start,
				end,
				false,
				false);
	}

	@Override
	public String getFieldName() {
		return fieldName;
	}

	public TemporalQueryConstraint(
			final String fieldName,
			final Date start,
			final Date end,
			final boolean inclusiveLow,
			final boolean inclusiveHigh ) {
		super();
		this.fieldName = fieldName;
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
	public QueryFilter getFilter() {
		return new DateRangeFilter(
				fieldName,
				start,
				end,
				inclusiveLow,
				inclusiveHigh);
	}

	public QueryRanges getQueryRanges() {
		return new QueryRanges(
				new ByteArrayRange(
						new ByteArray(
								TemporalIndexStrategy.toIndexByte(start)),
						new ByteArray(
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
			final FilterableConstraints constraints ) {
		if (constraints instanceof TemporalQueryConstraint) {
			final TemporalQueryConstraint filterConstraints = (TemporalQueryConstraint) constraints;
			if (fieldName.equals(filterConstraints.fieldName)) {
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
						fieldName,
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
			final FilterableConstraints constraints ) {
		if (constraints instanceof TemporalQueryConstraint) {
			final TemporalQueryConstraint filterConstraints = (TemporalQueryConstraint) constraints;
			if (fieldName.equals(filterConstraints.fieldName)) {
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
						fieldName,
						newStart,
						newEnd,
						newInclusiveLow,
						newInclusiveHigh);
			}
		}
		return this;
	}
}
