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
package org.locationtech.geowave.core.geotime.store.query;

import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.threeten.extra.Interval;

/**
 * The Spatial Temporal Query class represents a query in three dimensions. The
 * constraint that is applied represents an intersection operation on the query
 * geometry AND a date range intersection based on startTime and endTime.
 *
 *
 */
public class TemporalQuery extends
		BasicQuery
{
	public TemporalQuery(
			final Interval[] intervals ) {
		super(
				createTemporalConstraints(intervals));
	}

	public TemporalQuery(
			final TemporalConstraints contraints ) {
		super(
				createTemporalConstraints(contraints));
	}

	public TemporalQuery() {
		super();
	}

	private static Constraints createTemporalConstraints(
			final TemporalConstraints temporalConstraints ) {
		final List<ConstraintSet> constraints = new ArrayList<>();
		for (final TemporalRange range : temporalConstraints.getRanges()) {
			constraints.add(new ConstraintSet(
					TimeDefinition.class,
					new ConstraintData(
							new NumericRange(
									range.getStartTime().getTime(),
									range.getEndTime().getTime()),
							false)));
		}
		return new Constraints(
				constraints);
	}

	private static Constraints createTemporalConstraints(
			final Interval[] intervals ) {
		final List<ConstraintSet> constraints = new ArrayList<>();
		for (final Interval range : intervals) {
			constraints.add(new ConstraintSet(
					TimeDefinition.class,
					new ConstraintData(
							new NumericRange(
									range.getStart().toEpochMilli(),
									range.getEnd().toEpochMilli()),
							false)));
		}
		return new Constraints(
				constraints);
	}
}
