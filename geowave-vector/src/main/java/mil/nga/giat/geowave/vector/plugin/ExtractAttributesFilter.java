package mil.nga.giat.geowave.vector.plugin;

import java.util.Arrays;
import java.util.LinkedList;

import org.geotools.filter.visitor.NullFilterVisitor;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
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
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
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

/**
 * This class can be used to get Geometry from an OpenGIS filter object. GeoWave
 * then uses this geometry to perform a spatial intersection query.
 * 
 */
public class ExtractAttributesFilter extends
		NullFilterVisitor
{

	public ExtractAttributesFilter() {
	}

	@Override
	public Object visit(
			final ExcludeFilter filter,
			final Object data ) {
		return true;
	}

	@Override
	public Object visit(
			final IncludeFilter filter,
			final Object data ) {
		return true;
	}



	@SuppressWarnings("deprecation")
	@Override
	public Object visit(
			final BBOX filter,
			final Object data ) {
			return new LinkedList();
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
		return new LinkedList();
	}

	@Override
	public Object visit(
			final And filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Not filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Or filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Contains filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Crosses filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final DWithin filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Equals filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Intersects filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Overlaps filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Touches filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Within filter,
			Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Add expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Divide expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Function expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Id filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final Multiply expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final NilExpression expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visit(
			final PropertyName expression,
			final Object data ) {
		return Arrays.asList(expression.getPropertyName());
	}

	@Override
	public Object visit(
			final Subtract expression,
			final Object data ) {
		return new LinkedList();
	}

	@Override
	public Object visitNullFilter(
			final Object data ) {
		return new LinkedList();
	}

}
