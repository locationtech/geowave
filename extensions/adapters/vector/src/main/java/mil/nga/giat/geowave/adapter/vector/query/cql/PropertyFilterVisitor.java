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
package mil.nga.giat.geowave.adapter.vector.query.cql;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.numeric.NumericEqualsConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanOrEqualToConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericLessThanConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericLessThanOrEqualToConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericQueryConstraint;
import mil.nga.giat.geowave.core.store.index.text.TextExactMatchFilter;
import mil.nga.giat.geowave.core.store.index.text.TextQueryConstraint;

import org.geotools.filter.visitor.NullFilterVisitor;
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

/**
 * CQL visitor to extract constraints for secondary indexing queries.
 * 
 * TODO: compare operators for text (e.g. <,>,<=,>=) TODO: Temporal
 * 
 */
public class PropertyFilterVisitor extends
		NullFilterVisitor
{

	public PropertyFilterVisitor() {
		super();
	}

	@Override
	public Object visit(
			final ExcludeFilter filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final IncludeFilter filter,
			final Object data ) {
		return new PropertyConstraintSet();
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
		return value;
	}

	@Override
	public Object visit(
			final And filter,
			final Object data ) {
		final PropertyConstraintSet constraints = new PropertyConstraintSet();
		for (final Filter f : filter.getChildren()) {
			final Object output = f.accept(
					this,
					data);
			if (output instanceof PropertyConstraintSet) {
				constraints.intersect((PropertyConstraintSet) output);
			}

		}
		return constraints;
	}

	@Override
	public Object visit(
			final Not filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Or filter,
			final Object data ) {
		final PropertyConstraintSet constraints = new PropertyConstraintSet();
		for (final Filter f : filter.getChildren()) {
			final Object output = f.accept(
					this,
					data);
			if (output instanceof PropertyConstraintSet) {
				constraints.union((PropertyConstraintSet) output);
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
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final AnyInteracts anyInteracts,
			final Object data ) {
		return new PropertyConstraintSet();
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
		return new PropertyConstraintSet();
	}

	// t1 = t2.start
	// t1.start = t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final Begins begins,
			final Object data ) {
		return new PropertyConstraintSet();

	}

	// t1.start = t2
	// t1.start = t2.start and t1.end > t2.end
	@Override
	public Object visit(
			final BegunBy begunBy,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t2.start < t1 < t2.end
	// t1.start > t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final During during,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.end = t2
	// t1.start < t2.start and t1.end = t2.end
	@Override
	public Object visit(
			final EndedBy endedBy,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1 = t2.end
	// t1.start > t2.start and t1.end = t2.end
	@Override
	public Object visit(
			final Ends ends,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.end = t2.start
	@Override
	public Object visit(
			final Meets meets,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.start = t2.end
	// met by
	@Override
	public Object visit(
			final MetBy metBy,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.start > t2.start and t1.start < t2.end and t1.end > t2.end
	@Override
	public Object visit(
			final OverlappedBy overlappedBy,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.start < t2 < t1.end
	// t1.start < t2.start and t2.end < t1.end
	@Override
	public Object visit(
			final TContains contains,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final TEquals equals,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	// t1.start < t2.start and t1.end > t2.start and t1.end < t2.end
	@Override
	public Object visit(
			final TOverlaps overlaps,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Id filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object data ) {
		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression().accept(
				this,
				data);
		final Object lower = filter.getLowerBoundary().accept(
				this,
				data);

		final Object upper = filter.getUpperBoundary().accept(
				this,
				data);

		if (lower instanceof Number) {

			return new PropertyConstraintSet(
					new NumericQueryConstraint(
							leftResult,
							(Number) lower,
							(Number) upper,
							true,
							true));
		}
		return new PropertyConstraintSet();

	}

	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object data ) {
		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression1().accept(
				this,
				data);

		final Object value = filter.getExpression2().accept(
				this,
				data);

		if (value instanceof Number) {
			return new PropertyConstraintSet(
					new NumericEqualsConstraint(
							leftResult,
							(Number) value));
		}
		else if (value instanceof String) {
			return new PropertyConstraintSet(
					new TextQueryConstraint(
							leftResult,
							(String) value,
							true));
		}
		else {
			return new PropertyConstraintSet();
		}

	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object data ) {
		return filter.getExpression1().accept(
				this,
				data);

	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object data ) {
		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression1().accept(
				this,
				data);

		final Object value = filter.getExpression2().accept(
				this,
				data);

		if (value instanceof Number) {
			return new PropertyConstraintSet(
					new NumericGreaterThanConstraint(
							leftResult,
							(Number) value));
		}
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object data ) {
		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression1().accept(
				this,
				data);
		final Object value = filter.getExpression2().accept(
				this,
				data);

		if (value instanceof Number) {
			return new PropertyConstraintSet(
					new NumericGreaterThanOrEqualToConstraint(
							leftResult,
							(Number) value));
		}
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object data ) {
		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression1().accept(
				this,
				data);
		final Object value = filter.getExpression2().accept(
				this,
				data);

		if (value instanceof Number) {
			return new PropertyConstraintSet(
					new NumericLessThanConstraint(
							leftResult,
							(Number) value));
		}
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object data ) {

		final ByteArrayId leftResult = (ByteArrayId) filter.getExpression1().accept(
				this,
				data);

		final Object value = filter.getExpression2().accept(
				this,
				data);

		if (value instanceof Number) {
			return new PropertyConstraintSet(
					new NumericLessThanOrEqualToConstraint(
							leftResult,
							(Number) value));
		}
		return new PropertyConstraintSet();

	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object data ) {

		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyIsNil filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final BBOX filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Contains filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Crosses filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final DWithin filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Equals filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Intersects filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Overlaps filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Touches filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Within filter,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visitNullFilter(
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final NilExpression expression,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Add expression,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Divide expression,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final Function expression,
			final Object data ) {
		return new PropertyConstraintSet();
	}

	@Override
	public Object visit(
			final PropertyName expression,
			final Object data ) {
		return new ByteArrayId(
				expression.getPropertyName());

	}

	@Override
	public Object visit(
			final Subtract expression,
			final Object data ) {
		return expression.accept(
				this,
				data);
	}

}
