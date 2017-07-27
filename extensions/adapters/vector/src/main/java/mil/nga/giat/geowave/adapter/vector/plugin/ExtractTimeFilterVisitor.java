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
package mil.nga.giat.geowave.adapter.vector.plugin;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.geotools.data.Query;
import org.geotools.filter.visitor.NullFilterVisitor;
import org.geotools.util.Converters;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;
import org.opengis.temporal.Instant;
import org.opengis.temporal.Period;
import org.opengis.temporal.Position;

import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;

/**
 * This class can be used to get Time range from an OpenGIS filter object.
 * GeoWave then uses this time range to perform a spatial intersection query.
 *
 * Only those time elements associated with an index are extracted. At the
 * moment, the adapter only supports temporal indexing on a single attribute or
 * a pair of attributes representing a time range.
 *
 */
public class ExtractTimeFilterVisitor extends
		NullFilterVisitor
{
	private final List<String[]> validParamRanges = new LinkedList<String[]>();

	private boolean approximation = false;

	public ExtractTimeFilterVisitor() {}

	public ExtractTimeFilterVisitor(
			final TimeDescriptors timeDescriptors ) {
		if (timeDescriptors.hasTime() && (timeDescriptors.getStartRange() != null)
				&& (timeDescriptors.getEndRange() != null)) {
			addRangeVariables(
					timeDescriptors.getStartRange().getLocalName(),
					timeDescriptors.getEndRange().getLocalName());
		}
	}

	public void addRangeVariables(
			final String start,
			final String end ) {
		validParamRanges.add(new String[] {
			start,
			end
		});
	}

	public TemporalConstraintsSet getConstraints(
			final Filter filter ) {
		final TemporalConstraintsSet constrainsSet = getRawConstraints(filter);
		constrainsSet.setExact(!approximation);
		for (final String[] range : validParamRanges) {
			if (constrainsSet.hasConstraintsFor(range[0]) || constrainsSet.hasConstraintsFor(range[1])) {
				final TemporalConstraints start = (constrainsSet.hasConstraintsFor(range[0])) ? constrainsSet
						.getConstraintsFor(range[0]) : constrainsSet.getConstraintsFor(range[1]);
				// Note: getConstraints has a side effect that is returns a
				// constraint--full range, if necessary
				// so if start and end are both not specific, the prior line
				// would create the end
				// thus sconstraints and econstraints will be identical
				final TemporalConstraints end = (constrainsSet.hasConstraintsFor(range[1])) ? constrainsSet
						.getConstraintsFor(range[1]) : start;

				constrainsSet.removeConstraints(
						range[0],
						range[1]);
				final TemporalConstraints constraintsForRange = constrainsSet.getConstraintsForRange(
						range[0],
						range[1]);
				constraintsForRange.replaceWithIntersections(new TemporalConstraints(
						new TemporalRange(
								start.getStartRange().getStartTime(),
								end.getEndRange().getEndTime()),
						constraintsForRange.getName()));
			}
		}
		return constrainsSet;
	}

	public TemporalConstraintsSet getConstraints(
			final Query query ) {
		return getConstraints(query.getFilter());

	}

	private TemporalConstraintsSet getRawConstraints(
			final Filter filter ) {
		final Object output = filter.accept(
				this,
				null);

		if (output instanceof TemporalConstraintsSet) {
			return (TemporalConstraintsSet) output;
		}
		else if (output instanceof ParameterTimeConstraint) {
			final ParameterTimeConstraint paramConstraint = (ParameterTimeConstraint) output;
			final TemporalConstraintsSet constraintSet = new TemporalConstraintsSet();
			constraintSet.getConstraintsFor(
					paramConstraint.getName()).replaceWithMerged(
					paramConstraint);
			return constraintSet;
		}
		return new TemporalConstraintsSet();
	}

	/**
	 * Produce an ReferencedEnvelope from the provided data parameter.
	 *
	 * @param data
	 * @return ReferencedEnvelope
	 */
	private TemporalConstraints btime(
			final Object data ) {

		if (data == null) {
			return null;
		}
		if (data instanceof Date) {
			return toSet(new TemporalRange(
					(Date) data,
					(Date) data));
		}
		else if (data instanceof Timestamp) {
			return toSet(new TemporalRange(
					(Timestamp) data,
					(Timestamp) data));
		}
		else if (data instanceof Number) {
			final long val = ((Number) data).longValue();
			return toSet(new TemporalRange(
					new Date(
							val),
					new Date(
							val)));
		}
		else if (data instanceof TemporalRange) {
			return toSet((TemporalRange) data);
		}
		else if (data instanceof TemporalConstraints) {
			return (TemporalConstraints) data;
		}
		else if (data instanceof Period) {
			// all periods are exclusive
			final Position beginPosition = ((Period) data).getBeginning().getPosition();
			final Position endPosition = ((Period) data).getEnding().getPosition();
			Date s = TemporalRange.START_TIME, e = TemporalRange.START_TIME;

			if (beginPosition.getDate() != null) {
				// make it exclusive on start
				s = new Date(
						beginPosition.getDate().getTime() + 1);
			}
			else if (beginPosition.getTime() != null) {
				// make it exclusive on start
				s = new Date(
						beginPosition.getTime().getTime() + 1);
			}

			if (endPosition.getDate() != null) {
				// make it exclusive on end
				e = new Date(
						endPosition.getDate().getTime() - 1);
			}
			else if (endPosition.getTime() != null) {
				// make it exclusive on end
				e = new Date(
						endPosition.getTime().getTime() - 1);
			}
			if (s.getTime() > e.getTime()) {
				return new TemporalConstraints();
			}
			return toSet(new TemporalRange(
					s,
					e));
		}
		else if (data instanceof Instant) {
			final Position beginPosition = ((Instant) data).getPosition();
			Date s = TemporalRange.START_TIME;
			if (beginPosition.getDate() != null) {
				s = beginPosition.getDate();
			}
			else if (beginPosition.getTime() != null) {
				s = beginPosition.getTime();
			}
			return toSet(new TemporalRange(
					s,
					s));
		}

		final Date convertedDate = Converters.convert(
				data,
				Date.class);
		if (convertedDate != null) {
			return btime(convertedDate);
		}
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final ExcludeFilter filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final IncludeFilter filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	private TemporalConstraints toSet(
			final TemporalRange range ) {
		final TemporalConstraints contraints = new TemporalConstraints();
		contraints.add(range);
		return contraints;
	}

	/**
	 * Please note we are only visiting literals involved in spatial operations.
	 *
	 * @param literal
	 *            , hopefully a Geometry or Envelope
	 * @param data
	 *            Incoming BoundingBox (or Envelope or CRS)
	 *
	 * @return ReferencedEnvelope updated to reflect literal
	 */
	@Override
	public Object visit(
			final Literal expression,
			final Object data ) {
		final Object value = expression.getValue();
		return btime(value);
	}

	@Override
	public Object visit(
			final And filter,
			final Object data ) {
		final TemporalConstraintsSet constraints = new TemporalConstraintsSet();
		for (final Filter f : filter.getChildren()) {
			final Object output = f.accept(
					this,
					data);
			if (output instanceof ParameterTimeConstraint) {
				final ParameterTimeConstraint ranges = (ParameterTimeConstraint) output;
				constraints.getConstraintsFor(
						ranges.getName()).replaceWithIntersections(
						ranges);
			}
			else if (output instanceof TemporalConstraintsSet) {
				final TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) output;
				for (final Map.Entry<String, TemporalConstraints> entry : rangeSet.getSet()) {
					constraints.getConstraintsFor(
							entry.getKey()).replaceWithIntersections(
							entry.getValue());
				}

			}
		}
		for (final String[] range : validParamRanges) {
			if (constraints.hasConstraintsFor(range[0]) && constraints.hasConstraintsFor(range[1])) {
				final TemporalConstraints start = constraints.getConstraintsFor(range[0]);
				final TemporalConstraints end = constraints.getConstraintsFor(range[1]);
				constraints.removeConstraints(
						range[0],
						range[1]);
				// TODO: make this logic more robust
				if (start.getEndRange().getEndTime().after(
						end.getStartRange().getStartTime())) {
					constraints.getConstraintsForRange(
							range[0],
							range[1]).add(
							new TemporalRange(
									end.getStartRange().getStartTime(),
									start.getEndRange().getEndTime()));
				}
				else {
					// if there are multiple non-instersecting ranges, this is
					// an approximation
					approximation |= start.getRanges().size() > 1 || end.getRanges().size() > 1;

					constraints.getConstraintsForRange(
							range[0],
							range[1]).add(
							new TemporalRange(
									start.getStartRange().getStartTime(),
									end.getEndRange().getEndTime()));
				}
			}
		}
		return constraints;
	}

	public boolean isApproximation() {
		return approximation;
	}

	@Override
	public Object visit(
			final Not filter,
			final Object data ) {
		final Object output = filter.getFilter().accept(
				this,
				data);
		if (output instanceof ParameterTimeConstraint) {
			return not((ParameterTimeConstraint) output);
		}
		else if (output instanceof TemporalConstraintsSet) {
			final TemporalConstraintsSet newRangeSet = new TemporalConstraintsSet();
			final TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) output;
			for (final Map.Entry<String, TemporalConstraints> entry : rangeSet.getSet()) {
				newRangeSet.getConstraintsFor(
						entry.getKey()).replaceWithMerged(
						not(entry.getValue()));
			}
			return newRangeSet;
		}
		return output;
	}

	private TemporalConstraints not(
			final TemporalConstraints constraints ) {
		final ParameterTimeConstraint notRanges = new ParameterTimeConstraint(
				constraints.getName());
		notRanges.empty();

		Date lastMax = TemporalRange.START_TIME;
		for (final TemporalRange range : constraints.getRanges()) {
			if (range.getStartTime().after(
					TemporalRange.START_TIME)) {
				notRanges.add(new TemporalRange(
						lastMax,
						new Date(
								range.getStartTime().getTime() - 1)));
			}
			lastMax = range.getEndTime();
		}
		if (!constraints.isEmpty() && (TemporalRange.END_TIME.after(constraints.getEndRange().getEndTime()))) {
			notRanges.add(new TemporalRange(
					lastMax,
					TemporalRange.END_TIME));
		}
		return notRanges;
	}

	@Override
	public Object visit(
			final Or filter,
			final Object data ) {
		final TemporalConstraintsSet constraints = new TemporalConstraintsSet();
		for (final Filter f : filter.getChildren()) {
			final Object output = f.accept(
					this,
					data);
			if (output instanceof ParameterTimeConstraint) {
				final ParameterTimeConstraint ranges = (ParameterTimeConstraint) output;
				constraints.getConstraintsFor(
						ranges.getName()).replaceWithMerged(
						ranges);
			}
			else if (output instanceof TemporalConstraintsSet) {
				final TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) output;
				for (final Map.Entry<String, TemporalConstraints> entry : rangeSet.getSet()) {
					constraints.getConstraintsFor(
							entry.getKey()).replaceWithMerged(
							entry.getValue());
				}
			}

		}

		return constraints;
	}

	// t1 > t2
	// t1.start > t2
	// t1 > t2.end
	// t1.start > t2.end
	@Override
	public Object visit(
			final After after,
			final Object data ) {
		final TemporalConstraints leftResult = btime(after.getExpression1().accept(
				this,
				data));
		final TemporalConstraints rightResult = btime(after.getExpression2().accept(
				this,
				data));

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property after value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getMaxOr(
									TemporalRange.START_TIME,
									1),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getMinOr(
									TemporalRange.END_TIME,
									-1)),
					rightResult.getName());
		}
		// property after property
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final AnyInteracts anyInteracts,
			final Object data ) {
		return new TemporalConstraints();
	}

	// t1 < t2
	// t1.end < t2
	// t1 < t2.start
	// t1.end < t2.start
	// t1.end < t2.start
	@Override
	public Object visit(
			final Before before,
			final Object data ) {
		final TemporalConstraints leftResult = btime(before.getExpression1().accept(
				this,
				data));
		final TemporalConstraints rightResult = btime(before.getExpression2().accept(
				this,
				data));

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property before value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMinOr(
									TemporalRange.END_TIME,
									-1)),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							leftResult.getMaxOr(
									TemporalRange.START_TIME,
									1),
							TemporalRange.END_TIME),
					rightResult.getName());
		}
		// property after property
		return new TemporalConstraints();
	}

	// t1 = t2.start
	// t1.start = t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final Begins begins,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) begins.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) begins.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property begins value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					rightResult.getRanges(),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							leftResult.getMinOr(
									TemporalRange.START_TIME,
									0),
							TemporalRange.END_TIME),
					rightResult.getName());
		}
		// property begins property
		return new TemporalConstraints();

	}

	// t1.start = t2
	// t1.start = t2.start and t1.end > t2.end
	@Override
	public Object visit(
			final BegunBy begunBy,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) begunBy.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) begunBy.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property begun by value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getMinOr(
									TemporalRange.START_TIME,
									0),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					leftResult.getRanges(),
					rightResult.getName());
		}
		// property begins property
		return new TemporalConstraints();
	}

	// t2.start < t1 < t2.end
	// t1.start > t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final During during,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) during.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) during.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property during value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					rightResult.getRanges(),
					leftResult.getName());
		}
		// value during property
		else if (rightResult instanceof ParameterTimeConstraint) {
			return rightResult;
		}
		// property during property
		return new TemporalConstraints();
	}

	// t1.end = t2
	// t1.start < t2.start and t1.end = t2.end
	@Override
	public Object visit(
			final EndedBy endedBy,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) endedBy.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) endedBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property ended by value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(
									TemporalRange.END_TIME,
									0)),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					leftResult.getRanges(),
					rightResult.getName());
		}
		// property ended by property
		return new TemporalConstraints();
	}

	// t1 = t2.end
	// t1.start > t2.start and t1.end = t2.end
	@Override
	public Object visit(
			final Ends ends,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) ends.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) ends.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					rightResult.getRanges(),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getMaxOr(
									TemporalRange.END_TIME,
									0)),
					rightResult.getName());
		}
		// property ended by property
		return new TemporalConstraints();
	}

	// t1.end = t2.start
	@Override
	public Object visit(
			final Meets meets,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) meets.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) meets.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMinOr(
									TemporalRange.END_TIME,
									0)),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					rightResult.getName());
		}
		// property ended by property
		return new TemporalConstraints();
	}

	// t1.start = t2.end
	// met by
	@Override
	public Object visit(
			final MetBy metBy,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) metBy.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) metBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getMaxOr(
									TemporalRange.START_TIME,
									0),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getMinOr(
									TemporalRange.END_TIME,
									0)),
					rightResult.getName());
		}
		// property ends property
		return new TemporalConstraints();
	}

	// t1.start > t2.start and t1.start < t2.end and t1.end > t2.end
	@Override
	public Object visit(
			final OverlappedBy overlappedBy,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) overlappedBy.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) overlappedBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property overlappedBy value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getMinOr(
									TemporalRange.START_TIME,
									1),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getMaxOr(
									TemporalRange.END_TIME,
									-1)),
					rightResult.getName());
		}
		// property overlappedBy property
		return new TemporalConstraints();
	}

	// t1.start < t2 < t1.end
	// t1.start < t2.start and t2.end < t1.end
	@Override
	public Object visit(
			final TContains contains,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) contains.getExpression1().accept(
				this,
				data);

		final TemporalConstraints rightResult = (TemporalConstraints) contains.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property contains value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(
									TemporalRange.END_TIME,
									-1)),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					leftResult.getRanges(),
					rightResult.getName());
		}
		// property contains property
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final TEquals equals,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) equals.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) equals.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		// property contains value
		if (leftResult instanceof ParameterTimeConstraint) {
			return rightResult;
		}
		// value contains property
		if (rightResult instanceof ParameterTimeConstraint) {
			return leftResult;
		}
		// property contains property
		return new TemporalConstraints();
	}

	// t1.start < t2.start and t1.end > t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final TOverlaps overlaps,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) overlaps.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) overlaps.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}
		// according to geotools documentation this is exclusive even though
		// "overlaps" seems it should imply inclusive

		// property overlappedBy value
		if (leftResult instanceof ParameterTimeConstraint) {
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(
									TemporalRange.END_TIME,
									-1)),
					leftResult.getName());
		}
		else if (rightResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							leftResult.getMaxOr(
									TemporalRange.START_TIME,
									-1),
							TemporalRange.END_TIME),
					rightResult.getName());
		}
		// property overlappedBy property
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Id filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object data ) {
		final TemporalConstraints propertyExp = (TemporalConstraints) filter.getExpression().accept(
				this,
				data);

		final TemporalConstraints lowerBound = (TemporalConstraints) filter.getLowerBoundary().accept(
				this,
				data);
		final TemporalConstraints upperBound = (TemporalConstraints) filter.getUpperBoundary().accept(
				this,
				data);

		if (propertyExp.isEmpty()) {
			return new TemporalConstraints();
		}

		return new ParameterTimeConstraint(
				new TemporalRange(
						lowerBound.getStartRange().getStartTime(),
						upperBound.getEndRange().getEndTime()),
				propertyExp.getName());
	}

	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getStartRange().getStartTime(),
							rightResult.getEndRange().getEndTime()),
					leftResult.getName());
		}
		else {
			return new ParameterTimeConstraint(
					new TemporalRange(
							leftResult.getStartRange().getStartTime(),
							leftResult.getEndRange().getEndTime()),
					rightResult.getName());
		}
	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}
		if (leftResult instanceof ParameterTimeConstraint) {
			final ParameterTimeConstraint constraints = new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getStartRange().getStartTime()),
					leftResult.getName());
			constraints.add(new TemporalRange(
					rightResult.getEndRange().getEndTime(),
					TemporalRange.END_TIME));
			return constraints;
		}
		else {
			final ParameterTimeConstraint constraints = new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getStartRange().getStartTime()),
					rightResult.getName());
			constraints.add(new TemporalRange(
					leftResult.getEndRange().getEndTime(),
					TemporalRange.END_TIME));
			return constraints;
		}

	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}
		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							new Date(
									rightResult.getStartRange().getStartTime().getTime() + 1),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							new Date(
									leftResult.getStartRange().getStartTime().getTime() - 1)),
					rightResult.getName());
		}
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							rightResult.getStartRange().getStartTime(),
							TemporalRange.END_TIME),
					leftResult.getName());
		}
		else {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							leftResult.getStartRange().getStartTime()),
					rightResult.getName());
		}
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							new Date(
									rightResult.getStartRange().getStartTime().getTime() - 1)),
					leftResult.getName());
		}
		else {
			return new ParameterTimeConstraint(
					new TemporalRange(
							new Date(
									leftResult.getStartRange().getStartTime().getTime() + 1),
							TemporalRange.END_TIME),
					rightResult.getName());
		}
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object data ) {
		final TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		final TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) {
			return new TemporalConstraints();
		}

		if (leftResult instanceof ParameterTimeConstraint) {
			return new ParameterTimeConstraint(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getStartRange().getStartTime()),
					leftResult.getName());
		}
		else {
			return new ParameterTimeConstraint(
					new TemporalRange(
							leftResult.getStartRange().getStartTime(),
							TemporalRange.END_TIME),
					rightResult.getName());
		}
	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final PropertyIsNil filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final BBOX filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Contains filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Crosses filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final DWithin filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Equals filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Intersects filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Overlaps filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Touches filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Within filter,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visitNullFilter(
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final NilExpression expression,
			final Object data ) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Add expression,
			final Object data ) {
		return expression.accept(
				this,
				data);
	}

	@Override
	public Object visit(
			final Divide expression,
			final Object data ) {
		return expression.accept(
				this,
				data);
	}

	@Override
	public Object visit(
			final Function expression,
			final Object data ) {
		// used force full range if the expression contains a time
		// property...which is correct?
		return new TemporalConstraints();
	}

	private boolean validateName(
			final String name ) {
		return true;
	}

	@Override
	public Object visit(
			final PropertyName expression,
			final Object data ) {
		final String name = expression.getPropertyName();
		if (validateName(expression.getPropertyName())) {
			// for (final String[] range : validParamRanges) {
			// if (range[0].equals(name) || range[1].equals(name)) {
			// return new ParameterTimeConstraint(
			// range[0] + "_" + range[1]);
			// }
			// }
			return new ParameterTimeConstraint(
					name);
		}
		return new TemporalConstraints();
	}

	@Override
	public Object visit(
			final Subtract expression,
			final Object data ) {
		return expression.accept(
				this,
				data);
	}

	private boolean expressionContainsTime(
			final Expression expression ) {
		return !((TemporalConstraints) expression.accept(
				this,
				null)).isEmpty();
	}

	private boolean containsTime(
			final Function function ) {
		boolean yes = false;
		for (final Expression expression : function.getParameters()) {
			yes |= expressionContainsTime(expression);
		}
		return yes;
	}

	private static class ParameterTimeConstraint extends
			TemporalConstraints
	{

		public ParameterTimeConstraint(
				final String name ) {
			super(
					TemporalConstraints.FULL_RANGE,
					name);
		}

		public ParameterTimeConstraint(
				final List<TemporalRange> ranges,
				final String name ) {
			super(
					ranges,
					name);
		}

		public ParameterTimeConstraint(
				final TemporalRange range,
				final String name ) {
			super(
					range,
					name);
		}

		public TemporalConstraints bounds(
				final TemporalConstraints other ) {
			return other;
		}
	}
}
