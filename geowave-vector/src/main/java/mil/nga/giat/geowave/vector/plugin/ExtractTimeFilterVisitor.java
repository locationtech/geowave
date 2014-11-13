package mil.nga.giat.geowave.vector.plugin;

import java.sql.Timestamp;
import java.util.Date;

import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalRange;

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

/**
 * This class can be used to get Geometry from an OpenGIS filter object. GeoWave
 * then uses this geometry to perform a spatial intersection query.
 * 
 */
public class ExtractTimeFilterVisitor extends NullFilterVisitor {
	static public NullFilterVisitor TIME_VISITOR = new ExtractTimeFilterVisitor();

	/**
	 * This FilterVisitor is stateless - use
	 * ExtractGeometryFilterVisitor.BOUNDS_VISITOR. You may also subclass in
	 * order to reuse this functionality in your own FilterVisitor
	 * implementation.
	 */
	protected ExtractTimeFilterVisitor() {
	}

	/**
	 * Produce an ReferencedEnvelope from the provided data parameter.
	 * 
	 * @param data
	 * @return ReferencedEnvelope
	 */
	private TemporalConstraints btime(final Object data) {

		if (data == null) {
			return null;
		}
		if (data instanceof Date) {
			return toSet(new TemporalRange((Date) data, (Date) data));
		} else if (data instanceof Timestamp) {
			return toSet(new TemporalRange((Timestamp) data, (Timestamp) data));
		} else if (data instanceof Number) {
			long val = ((Number) data).longValue();
			return toSet(new TemporalRange(new Date(val), new Date(val)));
		} else if (data instanceof TemporalRange) {
			return toSet((TemporalRange) data);
		} else if (data instanceof TemporalConstraints) {
			return (TemporalConstraints) data;
		} else if (data instanceof Period) {
			final Position beginPosition = ((Period) data).getBeginning()
					.getPosition();
			final Position endPosition = ((Period) data).getEnding()
					.getPosition();
			Date s = TemporalRange.START_TIME, e = TemporalRange.START_TIME;

			if (beginPosition.getDate() != null)
				s = beginPosition.getDate();
			else if (beginPosition.getTime() != null)
				s = beginPosition.getTime();

			if (endPosition.getDate() != null)
				e = endPosition.getDate();
			else if (endPosition.getTime() != null)
				e = endPosition.getTime();
			return toSet(new TemporalRange(s, e));
		} else if (data instanceof Instant) {
			final Position beginPosition = ((Instant) data).getPosition();
			Date s = TemporalRange.START_TIME;
			if (beginPosition.getDate() != null)
				s = beginPosition.getDate();
			else if (beginPosition.getTime() != null)
				s = beginPosition.getTime();
			return toSet(new TemporalRange(s, s));
		}

		throw new ClassCastException("Could not cast data to TemporalRange");
	}

	@Override
	public Object visit(final ExcludeFilter filter, final Object data) {
		return new TemporalConstraints();
	}

	@Override
	public Object visit(final IncludeFilter filter, final Object data) {
		return infinity();
	}

	private TemporalConstraints infinity() {
		return new TemporalConstraints();
		//toSet(new TemporalRange(TemporalRange.START_TIME, new Date(
		//		Long.MAX_VALUE)));
	}

	private TemporalConstraints toSet(TemporalRange range) {
		TemporalConstraints contraints = new TemporalConstraints();
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
	public Object visit(final Literal expression, final Object data) {
		final Object value = expression.getValue();
		return btime(value);
	}

	@Override
	public Object visit(final And filter, final Object data) {
		TemporalConstraints ranges;
		TemporalConstraints lastRanges = null;
		for (final Filter f : filter.getChildren()) {
			ranges = (TemporalConstraints) f.accept(this, data);
			if (lastRanges != null) {
				lastRanges = TemporalConstraints.findIntersections(lastRanges,
						ranges);
			} else {
				lastRanges = ranges;
			}

		}
		return lastRanges;
	}

	@Override
	public Object visit(final Not filter, final Object data) {
		TemporalConstraints contraints = (TemporalConstraints) filter
				.getFilter().accept(this, data);
		TemporalConstraints notRanges = new TemporalConstraints();
		Date lastMax = TemporalRange.START_TIME;
		for (TemporalRange range : contraints.getRanges()) {
			if (range.getStartTime().after(TemporalRange.START_TIME)) {
				notRanges.add(new TemporalRange(lastMax, new Date(range
						.getStartTime().getTime() - 1)));
			}
			lastMax = range.getEndTime();
		}
		if (!contraints.isEmpty()
				&& (TemporalRange.END_TIME.after(contraints.getEndRange()
						.getEndTime())))
			notRanges.add(new TemporalRange(lastMax, TemporalRange.END_TIME));
		return notRanges;
	}

	@Override
	public Object visit(final Or filter, final Object data) {
		TemporalConstraints ranges;
		TemporalConstraints newSetOfRanges = new TemporalConstraints();

		for (final Filter f : filter.getChildren()) {
			ranges = (TemporalConstraints) f.accept(this, data);
			for (TemporalRange range : ranges.getRanges()) {
				newSetOfRanges.add(range);
			}
		}
		return newSetOfRanges;
	}

	public Object visit(After after, Object data) {
		TemporalConstraints side1 = btime(after.getExpression1().accept(this,
				data));
		TemporalConstraints side2 = btime(after.getExpression2().accept(this,
				data));

		if (!side2.isEmpty())
			return toSet(new TemporalRange(side2.getEndRange().getEndTime(),
					TemporalRange.END_TIME));
		else if (!side1.isEmpty())
			return toSet(new TemporalRange(side1.getEndRange().getEndTime(),
					TemporalRange.END_TIME));
		return side2;
	}

	public Object visit(AnyInteracts anyInteracts, Object data) {
		return infinity();
	}

	public Object visit(Before before, Object data) {
		TemporalConstraints side1 = btime(before.getExpression1().accept(this,
				data));
		TemporalConstraints side2 = btime(before.getExpression2().accept(this,
				data));

		if (!side2.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME, side2
					.getStartRange().getStartTime()));
		else if (!side1.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME, side1
					.getStartRange().getStartTime()));
		return side2;
	}

	public Object visit(Begins begins, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) begins
				.getExpression1().accept(this, data);

		TemporalConstraints rightResult = (TemporalConstraints) begins
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return toSet(new TemporalRange(
					rightResult.getMinOr(TemporalRange.START_TIME),
					TemporalRange.END_TIME));
		if (rightResult.isEmpty())
			return toSet(new TemporalRange(
					leftResult.getMinOr(TemporalRange.START_TIME),
					TemporalRange.END_TIME));

		// Looks like infinity. This case occurs if both sides are parameters or
		// functions.
		return infinity();
	}

	public Object visit(BegunBy begunBy, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) begunBy
				.getExpression1().accept(this, data);

		TemporalConstraints rightResult = (TemporalConstraints) begunBy
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return toSet(new TemporalRange(
					rightResult.getMinOr(TemporalRange.START_TIME),
					TemporalRange.END_TIME));
		if (rightResult.isEmpty())
			return toSet(new TemporalRange(
					leftResult.getMinOr(TemporalRange.START_TIME),
					TemporalRange.END_TIME));

		// Looks like infinity. This case occurs if both sides are parameters or
		// functions.
		return infinity();
	}

	public Object visit(During during, Object data) {
		return btime(during.getExpression2().accept(this, data));
	}

	public Object visit(EndedBy endedBy, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) endedBy
				.getExpression1().accept(this, data);

		TemporalConstraints rightResult = (TemporalConstraints) endedBy
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME,
					rightResult.getMaxOr(TemporalRange.END_TIME)));
		if (rightResult.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME,
					leftResult.getMaxOr(TemporalRange.END_TIME)));

		// Looks like infinity. This case occurs if both sides are parameters or
		// functions.
		return infinity();
	}

	public Object visit(Ends ends, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) ends
				.getExpression1().accept(this, data);

		TemporalConstraints rightResult = (TemporalConstraints) ends
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME,
					rightResult.getMaxOr(TemporalRange.END_TIME)));
		if (rightResult.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME,
					leftResult.getMaxOr(TemporalRange.END_TIME)));

		// Looks like infinity. This case occurs if both sides are parameters or
		// functions.
		return infinity();
	}

	public Object visit(Meets meets, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) meets
				.getExpression1().accept(this, data);

		TemporalConstraints rightResult = (TemporalConstraints) meets
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return toSet(new TemporalRange(TemporalRange.START_TIME,
					rightResult.getMinOr(TemporalRange.END_TIME)));
		if (rightResult.isEmpty())
			return toSet(new TemporalRange(
					leftResult.getMaxOr(TemporalRange.START_TIME),
					TemporalRange.END_TIME));

		// Looks like infinity. This case occurs if both sides are parameters or
		// functions.
		return toSet(new TemporalRange(
				leftResult.getMinOr(TemporalRange.START_TIME),
				rightResult.getMaxOr(TemporalRange.END_TIME)));
	}

	public Object visit(MetBy metBy, Object data) {
		return merge(metBy.getExpression1(), metBy.getExpression2(), data);
	}

	public Object visit(OverlappedBy overlappedBy, Object data) {
		TemporalConstraints side2 = btime(overlappedBy.getExpression2().accept(
				this, data));
		if (!side2.isEmpty())
			return side2;
		return this.infinity();
	}

	public Object visit(TContains contains, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) contains
				.getExpression1().accept(this, data);

		if (!leftResult.isEmpty())
			return leftResult;
		return this.infinity();
	}

	public Object visit(TEquals equals, Object data) {
		TemporalConstraints leftResult = (TemporalConstraints) equals
				.getExpression1().accept(this, data);
		TemporalConstraints rightResult = (TemporalConstraints) equals
				.getExpression2().accept(this, data);

		if (leftResult.isEmpty())
			return leftResult;
		if (rightResult.isEmpty())
			return this.infinity();
		return rightResult;
	}

	public Object visit(TOverlaps contains, Object data) {
		return merge(contains.getExpression1(), contains.getExpression2(), data);
	}

	public Object visit(Id filter, Object data) {
		return this.infinity();
	}

	public Object visit(PropertyIsBetween filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsEqualTo filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsNotEqualTo filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsGreaterThan filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsLessThan filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsLessThanOrEqualTo filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsLike filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsNull filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(PropertyIsNil filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(final BBOX filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Beyond filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Contains filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Crosses filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Disjoint filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(DWithin filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Equals filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Intersects filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Overlaps filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Touches filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Within filter, Object data) {
		return new TemporalConstraints();
	}

	public Object visitNullFilter(Object data) {
		return new TemporalConstraints();
	}

	public Object visit(NilExpression expression, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Add expression, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Divide expression, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Function expression, Object data) {
		if (containsTime(expression))
			return this.infinity();
		return new TemporalConstraints();
	}

	public Object visit(PropertyName expression, Object data) {
		return new TemporalConstraints();
	}

	public Object visit(Subtract expression, Object data) {
		return data;
	}

	private TemporalConstraints merge(Expression left, Expression right,
			Object data) {
		TemporalConstraints leftresult = (TemporalConstraints) left.accept(
				this, data);

		TemporalConstraints rightResult = (TemporalConstraints) right.accept(
				this, data);

		return TemporalConstraints.merge(leftresult, rightResult);
	}

	private boolean expressionContainsTime(Expression expression) {
		return ((TemporalConstraints) expression.accept(this, null)).isEmpty();
	}

	private boolean containsTime(Function function) {
		boolean yes = false;
		for (Expression expression : function.getParameters()) {
			yes |= this.expressionContainsTime(expression);
		}
		return yes;
	}

}
