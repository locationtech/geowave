package mil.nga.giat.geowave.vector.plugin;

import java.sql.Timestamp;
import java.util.Date;

import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalRange;
import mil.nga.giat.geowave.vector.utils.TimeDescriptors;

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
public class ExtractTimeFilterVisitor extends
		NullFilterVisitor
{
	static public NullFilterVisitor TIME_VISITOR = new ExtractTimeFilterVisitor();

	private TimeDescriptors timeDescriptor = null;

	public ExtractTimeFilterVisitor() {}

	public ExtractTimeFilterVisitor(
			TimeDescriptors timeDescriptor ) {
		this.timeDescriptor = timeDescriptor;
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
			long val = ((Number) data).longValue();
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
			final Position beginPosition = ((Period) data).getBeginning().getPosition();
			final Position endPosition = ((Period) data).getEnding().getPosition();
			Date s = TemporalRange.START_TIME, e = TemporalRange.START_TIME;

			if (beginPosition.getDate() != null)
				s = beginPosition.getDate();
			else if (beginPosition.getTime() != null) s = beginPosition.getTime();

			if (endPosition.getDate() != null)
				e = endPosition.getDate();
			else if (endPosition.getTime() != null) e = endPosition.getTime();
			return toSet(new TemporalRange(
					s,
					e));
		}
		else if (data instanceof Instant) {
			final Position beginPosition = ((Instant) data).getPosition();
			Date s = TemporalRange.START_TIME;
			if (beginPosition.getDate() != null)
				s = beginPosition.getDate();
			else if (beginPosition.getTime() != null) s = beginPosition.getTime();
			return toSet(new TemporalRange(
					s,
					s));
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
			TemporalRange range ) {
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
		TemporalConstraints ranges;
		TemporalConstraints lastRanges = null;
		for (final Filter f : filter.getChildren()) {
			ranges = (TemporalConstraints) f.accept(
					this,
					data);
			if (lastRanges != null) {
				lastRanges = TemporalConstraints.findIntersections(
						lastRanges,
						ranges);
			}
			else {
				lastRanges = ranges;
			}

		}
		return lastRanges;
	}

	@Override
	public Object visit(
			final Not filter,
			final Object data ) {
		TemporalConstraints contraints = (TemporalConstraints) filter.getFilter().accept(
				this,
				data);
		TemporalConstraints notRanges = new TemporalConstraints();
		Date lastMax = TemporalRange.START_TIME;
		for (TemporalRange range : contraints.getRanges()) {
			if (range.getStartTime().after(
					TemporalRange.START_TIME)) {
				notRanges.add(new TemporalRange(
						lastMax,
						new Date(
								range.getStartTime().getTime() - 1)));
			}
			lastMax = range.getEndTime();
		}
		if (!contraints.isEmpty() && (TemporalRange.END_TIME.after(contraints.getEndRange().getEndTime()))) notRanges.add(new TemporalRange(
				lastMax,
				TemporalRange.END_TIME));
		return notRanges;
	}

	@Override
	public Object visit(
			final Or filter,
			final Object data ) {
		TemporalConstraints ranges;
		TemporalConstraints newSetOfRanges = new TemporalConstraints();

		for (final Filter f : filter.getChildren()) {
			ranges = (TemporalConstraints) f.accept(
					this,
					data);
			for (TemporalRange range : ranges.getRanges()) {
				newSetOfRanges.add(range);
			}
		}
		return newSetOfRanges;
	}

	// t1 > t2
	// t1.start > t2
	// t1 > t2.end
	// t1.start > t2.end
	public Object visit(
			After after,
			Object data ) {
		TemporalConstraints leftResult = btime(after.getExpression1().accept(
				this,
				data));
		TemporalConstraints rightResult = btime(after.getExpression2().accept(
				this,
				data));

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property after value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							rightResult.getMaxOr(TemporalRange.START_TIME),
							TemporalRange.END_TIME));
		// value after property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						leftResult.getMinOr(TemporalRange.END_TIME)));
		// property after property
		return new ParameterTimeConstraint();
	}

	public Object visit(
			AnyInteracts anyInteracts,
			Object data ) {
		return new TemporalConstraints();
	}

	// t1 < t2
	// t1.end < t2
	// t1 < t2.start
	// t1.end < t2.start
	// t1.end < t2.start
	public Object visit(
			Before before,
			Object data ) {
		TemporalConstraints leftResult = btime(before.getExpression1().accept(
				this,
				data));
		TemporalConstraints rightResult = btime(before.getExpression2().accept(
				this,
				data));

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property before value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMinOr(TemporalRange.END_TIME)));
		// value after property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						leftResult.getMaxOr(TemporalRange.START_TIME),
						TemporalRange.END_TIME));
		// property after property
		return new ParameterTimeConstraint();
	}

	// t1 = t2.start
	// t1.start = t2.start and t1.end < t2.end
	public Object visit(
			Begins begins,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) begins.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) begins.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property begins value
		if (leftResult instanceof ParameterTimeConstraint)
			return rightResult;
		// value begins property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						leftResult.getMinOr(TemporalRange.START_TIME),
						TemporalRange.END_TIME));
		// property begins property
		return new ParameterTimeConstraint();

	}

	// t1.start = t2
	// t1.start = t2.start and t1.end > t2.end
	public Object visit(
			BegunBy begunBy,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) begunBy.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) begunBy.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property begun by value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							rightResult.getMinOr(TemporalRange.START_TIME),
							TemporalRange.END_TIME));
		// value begun by property
		else if (rightResult instanceof ParameterTimeConstraint) return leftResult;
		// property begins property
		return new ParameterTimeConstraint();
	}

	// t2.start < t1 < t2.end
	// t1.start > t2.start and t1.end < t2.end
	public Object visit(
			During during,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) during.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) during.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property during value
		if (leftResult instanceof ParameterTimeConstraint) return rightResult;
		// value during property
		// property during property
		return new ParameterTimeConstraint();
	}

	// t1.end = t2
	// t1.start < t2.start and t1.end = t2.end
	public Object visit(
			EndedBy endedBy,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) endedBy.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) endedBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property ended by value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(TemporalRange.END_TIME)));
		// value ended by property
		else if (rightResult instanceof ParameterTimeConstraint) return leftResult;
		// property ended by property
		return new ParameterTimeConstraint();
	}

	// t1 = t2.end
	// t1.start > t2.start and t1.end = t2.end
	public Object visit(
			Ends ends,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) ends.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) ends.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint)
			return rightResult;
		// value ends property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						leftResult.getMaxOr(TemporalRange.END_TIME)));
		// property ended by property
		return new ParameterTimeConstraint();
	}

	// t1.end = t2.start
	public Object visit(
			Meets meets,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) meets.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) meets.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						rightResult.getMinOr(TemporalRange.END_TIME)));
		// value ends property
		// property ended by property
		return new ParameterTimeConstraint();
	}

	// t1.start = t2.end
	// met by
	public Object visit(
			MetBy metBy,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) metBy.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) metBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property ends value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							rightResult.getMaxOr(TemporalRange.START_TIME),
							TemporalRange.END_TIME));
		// value ends property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						leftResult.getMinOr(TemporalRange.END_TIME)));
		// property ends property
		return new ParameterTimeConstraint();
	}

	// t1.start > t2.start and t1.start < t2.end and t1.end > t2.end
	public Object visit(
			OverlappedBy overlappedBy,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) overlappedBy.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) overlappedBy.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property overlappedBy value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							rightResult.getMinOr(TemporalRange.START_TIME),
							TemporalRange.END_TIME));
		// value overlappedBy property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						leftResult.getMaxOr(TemporalRange.END_TIME)));
		// property overlappedBy property
		return new ParameterTimeConstraint();
	}

	// t1.start < t2 < t1.end
	// t1.start < t2.start and t2.end < t1.end
	public Object visit(
			TContains contains,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) contains.getExpression1().accept(
				this,
				data);

		TemporalConstraints rightResult = (TemporalConstraints) contains.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property contains value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(TemporalRange.END_TIME)));
		// value contains property
		else if (rightResult instanceof ParameterTimeConstraint) return leftResult;
		// property contains property
		return new ParameterTimeConstraint();
	}

	public Object visit(
			TEquals equals,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) equals.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) equals.getExpression2().accept(
				this,
				data);

		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property contains value
		if (leftResult instanceof ParameterTimeConstraint) return rightResult;
		// value contains property
		if (rightResult instanceof ParameterTimeConstraint) return leftResult;
		// property contains property
		return new ParameterTimeConstraint();
	}

	// t1.start < t2.start and t1.end > t2.start and t1.end < t2.end
	public Object visit(
			TOverlaps overlaps,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) overlaps.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) overlaps.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		// property overlappedBy value
		if (leftResult instanceof ParameterTimeConstraint)
			return new TemporalConstraints(
					new TemporalRange(
							TemporalRange.START_TIME,
							rightResult.getMaxOr(TemporalRange.END_TIME)));
		// value overlappedBy property
		else if (rightResult instanceof ParameterTimeConstraint) return new TemporalConstraints(
				new TemporalRange(
						leftResult.getMaxOr(TemporalRange.START_TIME),
						TemporalRange.END_TIME));
		// property overlappedBy property
		return new ParameterTimeConstraint();
	}

	public Object visit(
			Id filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			PropertyIsBetween filter,
			Object data ) {
		TemporalConstraints propertyExp = (TemporalConstraints) filter.getExpression().accept(
				this,
				data);

		TemporalConstraints lowerBound = (TemporalConstraints) filter.getLowerBoundary().accept(
				this,
				data);
		TemporalConstraints upperBound = (TemporalConstraints) filter.getUpperBoundary().accept(
				this,
				data);

		if (propertyExp.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						lowerBound.getStartRange().getStartTime(),
						upperBound.getEndRange().getEndTime()));
	}

	public Object visit(
			PropertyIsEqualTo filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						rightResult.getStartRange().getStartTime(),
						rightResult.getEndRange().getEndTime()));
	}

	public Object visit(
			PropertyIsNotEqualTo filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		TemporalConstraints constraints = new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						rightResult.getStartRange().getStartTime()));
		constraints.add(new TemporalRange(
				rightResult.getEndRange().getEndTime(),
				TemporalRange.END_TIME));
		return constraints;
	}

	public Object visit(
			PropertyIsGreaterThan filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						rightResult.getStartRange().getStartTime(),
						TemporalRange.END_TIME));
	}

	public Object visit(
			PropertyIsGreaterThanOrEqualTo filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						rightResult.getStartRange().getStartTime(),
						TemporalRange.END_TIME));
	}

	public Object visit(
			PropertyIsLessThan filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						rightResult.getStartRange().getStartTime()));
	}

	public Object visit(
			PropertyIsLessThanOrEqualTo filter,
			Object data ) {
		TemporalConstraints leftResult = (TemporalConstraints) filter.getExpression1().accept(
				this,
				data);
		TemporalConstraints rightResult = (TemporalConstraints) filter.getExpression2().accept(
				this,
				data);
		if (leftResult.isEmpty() || rightResult.isEmpty()) return new TemporalConstraints();

		return new TemporalConstraints(
				new TemporalRange(
						TemporalRange.START_TIME,
						rightResult.getStartRange().getStartTime()));
	}

	public Object visit(
			PropertyIsLike filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			PropertyIsNull filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			PropertyIsNil filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			final BBOX filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Beyond filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Contains filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Crosses filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Disjoint filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			DWithin filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Equals filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Intersects filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Overlaps filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Touches filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Within filter,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visitNullFilter(
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			NilExpression expression,
			Object data ) {
		return new TemporalConstraints();
	}

	public Object visit(
			Add expression,
			Object data ) {
		return expression.accept(
				this,
				data);
	}

	public Object visit(
			Divide expression,
			Object data ) {
		return expression.accept(
				this,
				data);
	}

	public Object visit(
			Function expression,
			Object data ) {
		return (containsTime(expression)) ? new TemporalConstraints(
				TemporalConstraints.FULL_RANGE) : new TemporalConstraints();
	}

	private boolean validateName(
			String name ) {
		return (this.timeDescriptor == null || (this.timeDescriptor.getTime() != null && this.timeDescriptor.getTime().getLocalName().equals(
				name))) || (this.timeDescriptor.getEndRange() != null && this.timeDescriptor.getEndRange().getLocalName().equals(
				name)) || (this.timeDescriptor.getEndRange() != null && this.timeDescriptor.getEndRange().getLocalName().equals(
				name));

	}

	public Object visit(
			PropertyName expression,
			Object data ) {
		return validateName(expression.getPropertyName()) ? new ParameterTimeConstraint() : new TemporalConstraints();
	}

	public Object visit(
			Subtract expression,
			Object data ) {
		return expression.accept(
				this,
				data);
	}

	private boolean expressionContainsTime(
			Expression expression ) {
		return !((TemporalConstraints) expression.accept(
				this,
				null)).isEmpty();
	}

	private boolean containsTime(
			Function function ) {
		boolean yes = false;
		for (Expression expression : function.getParameters()) {
			yes |= this.expressionContainsTime(expression);
		}
		return yes;
	}

	private class ParameterTimeConstraint extends
			TemporalConstraints
	{

		public ParameterTimeConstraint() {
			super(
					TemporalConstraints.FULL_RANGE);
		}

		public TemporalConstraints bounds(
				TemporalConstraints other ) {
			return other;
		}
	}
}
