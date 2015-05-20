package mil.nga.giat.geowave.adapter.vector.plugin;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.geotools.filter.visitor.NullFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
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
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * This class can be used to get Geometry from an OpenGIS filter object. GeoWave
 * then uses this geometry to perform a spatial intersection query.
 * 
 */
public class ExtractGeometryFilterVisitor extends
		NullFilterVisitor
{
	public static final NullFilterVisitor GEOMETRY_VISITOR = new ExtractGeometryFilterVisitor(
			GeoWaveGTDataStore.DEFAULT_CRS);

	private static Logger LOGGER = Logger.getLogger(ExtractGeometryFilterVisitor.class);

	private final CoordinateReferenceSystem crs;

	/**
	 * This FilterVisitor is stateless - use
	 * ExtractGeometryFilterVisitor.BOUNDS_VISITOR. You may also subclass in
	 * order to reuse this functionality in your own FilterVisitor
	 * implementation.
	 */
	protected ExtractGeometryFilterVisitor(
			final CoordinateReferenceSystem crs ) {
		this.crs = crs;
	}

	/**
	 * Produce an ReferencedEnvelope from the provided data parameter.
	 * 
	 * @param data
	 * @return ReferencedEnvelope
	 */
	private Geometry bbox(
			final Object data ) {
		try {
			if (data == null) {
				return null;
			}
			else if (data instanceof ReferencedEnvelope) {

				return new GeometryFactory().toGeometry(((ReferencedEnvelope) data).transform(
						crs,
						true));

			}
			else if (data instanceof Envelope) {
				return new GeometryFactory().toGeometry((Envelope) data);
			}
			else if (data instanceof CoordinateReferenceSystem) {
				return new GeometryFactory().toGeometry(new ReferencedEnvelope(
						(CoordinateReferenceSystem) data).transform(
						crs,
						true));
			}
		}
		catch (TransformException | FactoryException e) {
			LOGGER.warn(
					"Unable to transform geometry",
					e);
			return null;
		}
		throw new ClassCastException(
				"Could not cast data to ReferencedEnvelope");
	}

	@Override
	public Object visit(
			final ExcludeFilter filter,
			final Object data ) {
		return null;
	}

	@Override
	public Object visit(
			final IncludeFilter filter,
			final Object data ) {
		return infinity();
	}

	private Geometry infinity() {
		return GeometryUtils.infinity();
	}

	@SuppressWarnings("deprecation")
	@Override
	public Object visit(
			final BBOX filter,
			final Object data ) {
		final Geometry bbox = bbox(data);

		// consider doing reprojection here into data CRS?
		final Envelope bounds = new Envelope(
				filter.getMinX(),
				filter.getMaxX(),
				filter.getMinY(),
				filter.getMaxY());
		if (bbox != null) {
			return bbox.union(new GeometryFactory().toGeometry(bounds));
		}
		else {
			return bbox(bounds);
		}
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
		if (value instanceof Geometry) {
			final Geometry geometry = (Geometry) value;
			return geometry;
		}
		else {
			LOGGER.info("LiteralExpression ignored!");
		}
		return bbox(data);
	}

	@Override
	public Object visit(
			final And filter,
			final Object data ) {
		Geometry mixed = infinity();
		for (final Filter f : filter.getChildren()) {
			final Object obj = f.accept(
					this,
					data);
			if ((obj != null) && (obj instanceof Geometry)) {
				final Geometry geom = (Geometry) obj;
				final double mixedArea = mixed.getArea();
				final double geomArea = geom.getArea();
				if (Double.isInfinite(mixedArea) || Double.isNaN(mixedArea)) {
					mixed = geom;
				}
				else if (!Double.isInfinite(geomArea) && !Double.isNaN(geomArea)) {
					mixed = mixed.intersection(geom);
				}
			}
		}
		return mixed;
	}

	@Override
	public Object visit(
			final Not filter,
			final Object data ) {
		// no matter what we have to return an infinite envelope
		// rationale
		// !(finite envelope) -> an unbounded area -> infinite
		// !(non spatial filter) -> infinite (no spatial concern)
		// !(infinite) -> ... infinite, as the first infinite could be the
		// result
		// of !(finite envelope)

		return infinity();
	}

	@Override
	public Object visit(
			final Or filter,
			final Object data ) {
		Geometry mixed = new GeometryFactory().toGeometry(new Envelope());
		for (final Filter f : filter.getChildren()) {
			final Geometry geom = (Geometry) f.accept(
					this,
					data);
			final double geomArea = geom.getArea();
			if (!Double.isInfinite(geomArea) && !Double.isNaN(geomArea)) {
				mixed = mixed.union(geom);
			}
		}
		if (mixed.isEmpty()) {
			return infinity();
		}
		return mixed;
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object data ) {
		// beyond a certain distance from a finite object, no way to limit it
		return infinity();
	}

	@Override
	public Object visit(
			final Contains filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);
		return data;
	}

	@Override
	public Object visit(
			final Crosses filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);
		return data;
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object data ) {
		// disjoint does not define a rectangle, but a hole in the
		// Cartesian plane, no way to limit it
		return infinity();
	}

	@Override
	public Object visit(
			final DWithin filter,
			final Object data ) {
		final Geometry bbox = bbox(data);

		// we have to take the reference geometry bbox and
		// expand it by the distance.
		// We ignore the unit of measure for the moment
		Literal geometry = null;
		if ((filter.getExpression1() instanceof PropertyName) && (filter.getExpression2() instanceof Literal)) {
			geometry = (Literal) filter.getExpression2();
		}
		if ((filter.getExpression2() instanceof PropertyName) && (filter.getExpression1() instanceof Literal)) {
			geometry = (Literal) filter.getExpression2();
		}

		// we cannot desume a bbox from this filter
		if (geometry == null) {
			return infinity();
		}

		Geometry geom = geometry.evaluate(
				null,
				Geometry.class);
		if (geom == null) {
			return infinity();
		}
		Pair<Geometry, Double> geometryAndDegrees;
		try {
			geometryAndDegrees = mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils.buffer(
					crs,
					geom,
					filter.getDistanceUnits(),
					filter.getDistance());
		}
		catch (TransformException e) {
			LOGGER.error(
					"Cannot transform geometry to CRS",
					e);
			geometryAndDegrees = Pair.of(
					geom,
					filter.getDistance());
		}

		if (bbox != null) {
			return geometryAndDegrees.getLeft().union(
					bbox);
		}
		else {
			return geometryAndDegrees.getLeft();
		}
	}

	@Override
	public Object visit(
			final Equals filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);
		return data;
	}

	@Override
	public Object visit(
			final Intersects filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);

		return data;
	}

	@Override
	public Object visit(
			final Overlaps filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);

		return data;
	}

	@Override
	public Object visit(
			final Touches filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);

		return data;
	}

	@Override
	public Object visit(
			final Within filter,
			Object data ) {
		data = filter.getExpression1().accept(
				this,
				data);
		data = filter.getExpression2().accept(
				this,
				data);

		return data;
	}

	@Override
	public Object visit(
			final Add expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final Divide expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final Function expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final Id filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final Multiply expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final NilExpression expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visit(
			final PropertyName expression,
			final Object data ) {
		return null;
	}

	@Override
	public Object visit(
			final Subtract expression,
			final Object data ) {
		return infinity();
	}

	@Override
	public Object visitNullFilter(
			final Object data ) {
		return infinity();
	}

}
