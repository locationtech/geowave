package mil.nga.giat.geowave.adapter.vector.query.cql;

/**
 *    A modified copy of  org.geotools.filter.text.cql2.FilterToCQL from GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org (C) 2006-2008, Open Source Geospatial Foundation (OSGeo).
 *    
 *    Fixes unsupported negated equavalence (<> instead of !=).
 *    Fixes construction of the'in' ID expression where an id contains non-alpha characters '-','+', etc.
 */

import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.filter.spatial.IntersectsImpl;
import org.geotools.filter.text.commons.ExpressionToText;
import org.geotools.filter.text.commons.FilterToTextUtil;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
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
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.identity.Identifier;
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
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;

public class FilterToECQLExtension implements
		FilterVisitor
{

	private static Logger LOGGER = LoggerFactory.getLogger(FilterToECQLExtension.class);

	ExpressionToText expressionVisitor = new ExpressionToText();

	@Override
	public Object visitNullFilter(
			final Object extraData ) {
		throw new NullPointerException(
				"Cannot encode null as a Filter");
	}

	@Override
	public Object visit(
			final ExcludeFilter filter,
			final Object extraData ) {

		return FilterToTextUtil.buildExclude(extraData);
	}

	@Override
	public Object visit(
			final IncludeFilter filter,
			final Object extraData ) {
		return FilterToTextUtil.buildInclude(extraData);
	}

	@Override
	public Object visit(
			final And filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinaryLogicalOperator(
				"AND",
				this,
				filter,
				extraData);
	}

	/**
	 * builds a ecql id expression: in (id1, id2, ...)
	 */
	@Override
	public Object visit(
			final Id filter,
			final Object extraData ) {

		final StringBuilder ecql = FilterToTextUtil.asStringBuilder(extraData);
		ecql.append("IN (");

		final Iterator<Identifier> iter = filter.getIdentifiers().iterator();
		while (iter.hasNext()) {
			final Identifier identifier = iter.next();
			final String id = identifier.toString();

			// CHANGE FROM GEOTOOLS: look for identifiers with non-alphanumeric
			// characters
			final boolean needsQuotes = id.matches(".*(\\.|\\-|\\+|\\*|\\/).*");

			if (needsQuotes) {
				ecql.append('\'');
			}
			ecql.append(identifier);
			if (needsQuotes) {
				ecql.append('\'');
			}

			if (iter.hasNext()) {
				ecql.append(",");
			}
		}
		ecql.append(")");
		return ecql;
	}

	/**
	 * builds the Not logical operator
	 */
	@Override
	public Object visit(
			final Not filter,
			final Object extraData ) {
		return FilterToTextUtil.buildNot(
				this,
				filter,
				extraData);
	}

	/**
	 * Builds the OR logical operator.
	 * 
	 * This visitor checks for {@link #isInFilter(Or)} and is willing to output
	 * ECQL of the form <code>left IN (right, right, right)</code>.
	 */
	@Override
	public Object visit(
			final Or filter,
			final Object extraData ) {
		if (isInFilter(filter)) {
			return buildIN(
					filter,
					extraData);
		}
		// default to normal OR output
		return FilterToTextUtil.buildBinaryLogicalOperator(
				"OR",
				this,
				filter,
				extraData);
	}

	/** Check if this is an encoding of ECQL IN */
	private boolean isInFilter(
			final Or filter ) {
		if (filter.getChildren() == null) {
			return false;
		}
		Expression left = null;
		for (final Filter child : filter.getChildren()) {
			if (child instanceof PropertyIsEqualTo) {
				final PropertyIsEqualTo equal = (PropertyIsEqualTo) child;
				if (left == null) {
					left = equal.getExpression1();
				}
				else if (!left.equals(equal.getExpression1())) {
					return false; // not IN
				}
			}
			else {
				return false; // not IN
			}
		}
		return true;
	}

	private Object buildIN(
			final Or filter,
			final Object extraData ) {
		final StringBuilder output = FilterToTextUtil.asStringBuilder(extraData);
		final List<Filter> children = filter.getChildren();
		final PropertyIsEqualTo first = (PropertyIsEqualTo) filter.getChildren().get(
				0);
		final Expression left = first.getExpression1();
		left.accept(
				expressionVisitor,
				output);
		output.append(" IN (");
		for (final Iterator<Filter> i = children.iterator(); i.hasNext();) {
			final PropertyIsEqualTo child = (PropertyIsEqualTo) i.next();
			final Expression right = child.getExpression2();
			right.accept(
					expressionVisitor,
					output);
			if (i.hasNext()) {
				output.append(",");
			}
		}
		output.append(")");
		return output;
	}

	/**
	 * builds the BETWEEN predicate
	 */
	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBetween(
				filter,
				extraData);
	}

	/**
	 * Output EQUAL filter (will checks for ECQL geospatial operations).
	 */
	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object extraData ) {
		final StringBuilder output = FilterToTextUtil.asStringBuilder(extraData);
		if (isRelateOperation(filter)) {
			return buildRelate(
					filter,
					output);
		}
		else if (isFunctionTrue(
				filter,
				"PropertyExists",
				1)) {
			return buildExists(
					filter,
					output);
		}
		return FilterToTextUtil.buildComparison(
				filter,
				output,
				"=");
	}

	/** Check if this is an encoding of ECQL geospatial operation */
	private boolean isFunctionTrue(
			final PropertyIsEqualTo filter,
			final String operation,
			final int numberOfArguments ) {
		if (filter.getExpression1() instanceof Function) {
			final Function function = (Function) filter.getExpression1();
			final List<Expression> parameters = function.getParameters();
			if (parameters == null) {
				return false;
			}
			final String name = function.getName();
			if (!operation.equalsIgnoreCase(name) || (parameters.size() != numberOfArguments)) {
				return false;
			}
		}
		else {
			return false;
		}
		if (filter.getExpression2() instanceof Literal) {
			final Literal literal = (Literal) filter.getExpression2();
			final Boolean value = literal.evaluate(
					null,
					Boolean.class);
			if ((value == null) || (value == false)) {
				return false;
			}
		}
		else {
			return false;
		}
		return true;
	}

	private Object buildExists(
			final PropertyIsEqualTo filter,
			final StringBuilder output ) {
		final Function function = (Function) filter.getExpression1();
		final List<Expression> parameters = function.getParameters();
		final Literal arg = (Literal) parameters.get(0);

		output.append(arg.getValue());
		output.append(" EXISTS");
		return output;
	}

	/** Check if this is an encoding of ECQL geospatial operation */
	private boolean isRelateOperation(
			final PropertyIsEqualTo filter ) {
		if (isFunctionTrue(
				filter,
				"relatePattern",
				3)) {
			final Function function = (Function) filter.getExpression1();
			final List<Expression> parameters = function.getParameters();
			final Expression param3 = parameters.get(2);
			if (param3 instanceof Literal) {
				final Literal literal = (Literal) param3;
				final Object value = literal.getValue();
				if (!(value instanceof String)) {
					return false; // not a relate
				}
			}
		}
		else {
			return false;
		}
		return true;
	}

	private Object buildRelate(
			final PropertyIsEqualTo filter,
			final StringBuilder output ) {
		final Function operation = (Function) filter.getExpression1();
		output.append("RELATE(");
		final List<Expression> parameters = operation.getParameters();
		final Expression arg1 = parameters.get(0);
		final Expression arg2 = parameters.get(1);
		final Literal arg3 = (Literal) parameters.get(2);

		arg1.accept(
				expressionVisitor,
				output);
		output.append(",");
		arg2.accept(
				expressionVisitor,
				output);
		output.append(",");
		output.append(arg3.getValue());
		output.append(")");
		return output;
	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object extraData ) {

		// CHANGE FROM GEOTOOLS: <> instead of !=
		return FilterToTextUtil.buildComparison(
				filter,
				extraData,
				"<>");
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object extraData ) {
		return FilterToTextUtil.buildComparison(
				filter,
				extraData,
				">");
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object extraData ) {
		return FilterToTextUtil.buildComparison(
				filter,
				extraData,
				">=");
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object extraData ) {
		return FilterToTextUtil.buildComparison(
				filter,
				extraData,
				"<");
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object extraData ) {
		return FilterToTextUtil.buildComparison(
				filter,
				extraData,
				"<=");
	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object extraData ) {
		return FilterToTextUtil.buildIsLike(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object extraData ) {
		return FilterToTextUtil.buildIsNull(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsNil filter,
			final Object extraData ) {
		throw new UnsupportedOperationException(
				"PropertyIsNil not supported");
	}

	@Override
	public Object visit(
			final BBOX filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBBOX(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object extraData ) {
		return FilterToTextUtil.buildDistanceBufferOperation(
				"BEYOND",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Contains filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"CONTAINS",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Crosses filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"CROSSES",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"DISJOINT",
				filter,
				extraData);
	}

	/**
	 * DWithin spatial operator will find out if a feature in a datalayer is
	 * within X meters of a point, line, or polygon.
	 */
	@Override
	public Object visit(
			final DWithin filter,
			final Object extraData ) {
		IntersectsImpl newWithImpl = null;
		try {
			if ((filter.getExpression1() instanceof PropertyName) && (filter.getExpression2() instanceof Literal)) {
				Pair<Geometry, Double> geometryAndDegrees;

				geometryAndDegrees = mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils.buffer(
						GeoWaveGTDataStore.DEFAULT_CRS,
						filter.getExpression2().evaluate(
								extraData,
								Geometry.class),
						filter.getDistanceUnits(),
						filter.getDistance());

				newWithImpl = new IntersectsImpl(
						filter.getExpression1(),
						new LiteralExpressionImpl(
								geometryAndDegrees.getLeft()));

			}
			else if ((filter.getExpression2() instanceof PropertyName) && (filter.getExpression1() instanceof Literal)) {
				final Pair<Geometry, Double> geometryAndDegrees = mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils
						.buffer(
								GeoWaveGTDataStore.DEFAULT_CRS,
								filter.getExpression1().evaluate(
										extraData,
										Geometry.class),
								filter.getDistanceUnits(),
								filter.getDistance());
				newWithImpl = new IntersectsImpl(
						new LiteralExpressionImpl(
								geometryAndDegrees.getLeft()),
						filter.getExpression2());
			}
		}
		catch (TransformException e) {
			LOGGER.error(
					"Cannot transform geoemetry to support provide distance",
					e);
			return FilterToTextUtil.buildDWithin(
					filter,
					extraData);
		}
		return FilterToTextUtil.buildBinarySpatialOperator(
				"INTERSECTS",
				newWithImpl,
				extraData);
	}

	@Override
	public Object visit(
			final Equals filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"EQUALS",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Intersects filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"INTERSECTS",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Overlaps filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"OVERLAPS",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Touches filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"TOUCHES",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Within filter,
			final Object extraData ) {
		return FilterToTextUtil.buildBinarySpatialOperator(
				"WITHIN",
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final After after,
			final Object extraData ) {
		return FilterToTextUtil.buildBinaryTemporalOperator(
				"AFTER",
				after,
				extraData);
	}

	@Override
	public Object visit(
			final Before before,
			final Object extraData ) {
		return FilterToTextUtil.buildBinaryTemporalOperator(
				"BEFORE",
				before,
				extraData);
	}

	@Override
	public Object visit(
			final AnyInteracts anyInteracts,
			final Object extraData ) {
		throw ecqlUnsupported("AnyInteracts");
	}

	@Override
	public Object visit(
			final Begins begins,
			final Object extraData ) {
		throw ecqlUnsupported("Begins");
	}

	@Override
	public Object visit(
			final BegunBy begunBy,
			final Object extraData ) {
		throw ecqlUnsupported("BegunBy");
	}

	/**
	 * New instance of unsupported exception with the name of filter
	 * 
	 * @param filterName
	 *            filter unsupported
	 * @return UnsupportedOperationException
	 */
	static private UnsupportedOperationException ecqlUnsupported(
			final String filterName ) {
		return new UnsupportedOperationException(
				"The" + filterName + " has not an ECQL expression");
	}

	@Override
	public Object visit(
			final During during,
			final Object extraData ) {
		return FilterToTextUtil.buildDuring(
				during,
				extraData);
	}

	@Override
	public Object visit(
			final EndedBy endedBy,
			final Object extraData ) {
		throw ecqlUnsupported("EndedBy");
	}

	@Override
	public Object visit(
			final Ends ends,
			final Object extraData ) {
		throw ecqlUnsupported("EndedBy");
	}

	@Override
	public Object visit(
			final Meets meets,
			final Object extraData ) {
		throw ecqlUnsupported("Meets");
	}

	@Override
	public Object visit(
			final MetBy metBy,
			final Object extraData ) {
		throw ecqlUnsupported("MetBy");
	}

	@Override
	public Object visit(
			final OverlappedBy overlappedBy,
			final Object extraData ) {
		throw ecqlUnsupported("OverlappedBy");
	}

	@Override
	public Object visit(
			final TContains contains,
			final Object extraData ) {
		throw ecqlUnsupported("TContains");
	}

	@Override
	public Object visit(
			final TEquals equals,
			final Object extraData ) {
		throw ecqlUnsupported("TContains");
	}

	@Override
	public Object visit(
			final TOverlaps contains,
			final Object extraData ) {
		throw ecqlUnsupported("TContains");
	}
}
