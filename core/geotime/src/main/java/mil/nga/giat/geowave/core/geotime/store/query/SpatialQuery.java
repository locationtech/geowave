package mil.nga.giat.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter.BasicQueryCompareOperation;

/**
 * The Spatial Query class represents a query in two dimensions. The constraint
 * that is applied represents an intersection operation on the query geometry.
 * 
 */
public class SpatialQuery extends
		BasicQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialQuery.class);
	private Geometry queryGeometry;
	CompareOperation compareOp = CompareOperation.INTERSECTS;
	BasicQueryCompareOperation nonSpatialCompareOp = BasicQueryCompareOperation.INTERSECTS;

	/**
	 * Convenience constructor used to construct a SpatialQuery object that has
	 * an X and Y dimension (axis).
	 * 
	 * @param queryGeometry
	 *            spatial geometry of the query
	 */
	public SpatialQuery(
			final Geometry queryGeometry ) {
		super(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry));
		this.queryGeometry = queryGeometry;
	}

	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry ) {
		super(
				constraints);
		this.queryGeometry = queryGeometry;
	}

	public SpatialQuery(
			final Geometry queryGeometry,
			Map<ByteArrayId, FilterableConstraints> additionalConstraints ) {
		super(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
				additionalConstraints);
	}

	/**
	 * Convenience constructor used to construct a SpatialQuery object that has
	 * an X and Y dimension (axis).
	 * 
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param overlaps
	 *            if false, the only fully contained geometries are requested
	 */
	public SpatialQuery(
			final Geometry queryGeometry,
			final CompareOperation compareOp ) {
		super(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry));
		this.queryGeometry = queryGeometry;
		this.compareOp = compareOp;
	}

	/**
	 * Convenience constructor can be used when you already have linear
	 * constraints for the query. The queryGeometry and compareOp is used for
	 * fine grained post filtering.
	 * 
	 * @param constraints
	 *            linear constraints
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param compareOp
	 *            predicate associated query geometry
	 */
	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final CompareOperation compareOp ) {
		this(
				constraints,
				queryGeometry,
				compareOp,
				BasicQueryCompareOperation.INTERSECTS);
	}

	/**
	 * Convenience constructor can be used when you already have linear
	 * constraints for the query. The queryGeometry and compareOp is used for
	 * fine grained post filtering.
	 * 
	 * @param constraints
	 *            linear constraints
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param compareOp
	 *            predicate associated query geometry
	 * @param nonSpatialCompareOp
	 *            predicate associated non-spatial fields (i.e Time)
	 */
	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final CompareOperation compareOp,
			final BasicQueryCompareOperation nonSpatialCompareOp ) {
		super(
				constraints);
		this.queryGeometry = queryGeometry;
		this.compareOp = compareOp;
		this.nonSpatialCompareOp = nonSpatialCompareOp;
	}

	protected SpatialQuery() {
		super();
	}

	/**
	 * 
	 * @return queryGeometry the spatial geometry of the SpatialQuery object
	 */
	public Geometry getQueryGeometry() {
		return queryGeometry;
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			final NumericDimensionField<?>[] unconstrainedDimensionDefinitions ) {
		return new SpatialQueryFilter(
				constraints,
				orderedConstrainedDimensionFields,
				unconstrainedDimensionDefinitions,
				queryGeometry,
				compareOp,
				nonSpatialCompareOp);
	}

	@Override
	public byte[] toBinary() {
		final byte[] superBinary = super.toBinary();
		final byte[] geometryBinary = new WKBWriter().write(queryGeometry);
		final ByteBuffer buf = ByteBuffer.allocate(superBinary.length + geometryBinary.length + 3 * 4);
		buf.putInt(compareOp.ordinal());
		buf.putInt(nonSpatialCompareOp.ordinal());
		buf.putInt(superBinary.length);
		buf.put(superBinary);
		buf.put(geometryBinary);

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		compareOp = CompareOperation.values()[buf.getInt()];
		nonSpatialCompareOp = BasicQueryCompareOperation.values()[buf.getInt()];
		final int superLength = buf.getInt();
		final byte[] superBinary = new byte[superLength];
		buf.get(superBinary);
		super.fromBinary(superBinary);
		final byte[] geometryBinary = new byte[bytes.length - superLength - 3 * 4];
		buf.get(geometryBinary);
		try {
			queryGeometry = new WKBReader().read(geometryBinary);
		}
		catch (final ParseException e) {
			LOGGER.warn(
					"Unable to read query geometry as well-known binary",
					e);
		}
	}

}