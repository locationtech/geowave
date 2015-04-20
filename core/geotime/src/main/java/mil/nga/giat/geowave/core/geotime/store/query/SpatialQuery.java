package mil.nga.giat.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * The Spatial Query class represents a query in two dimensions. The constraint
 * that is applied represents an intersection operation on the query geometry.
 * 
 */
public class SpatialQuery extends
		BasicQuery
{
	private final static Logger LOGGER = Logger.getLogger(SpatialQuery.class);
	private Geometry queryGeometry;

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
	protected QueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields ) {
		return new SpatialQueryFilter(
				constraints,
				dimensionFields,
				queryGeometry);
	}

	@Override
	public byte[] toBinary() {
		final byte[] superBinary = super.toBinary();
		final byte[] geometryBinary = new WKBWriter().write(queryGeometry);
		final ByteBuffer buf = ByteBuffer.allocate(superBinary.length + geometryBinary.length + 4);
		buf.putInt(superBinary.length);
		buf.put(superBinary);
		buf.put(geometryBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int superLength = buf.getInt();
		final byte[] superBinary = new byte[superLength];
		buf.get(superBinary);
		super.fromBinary(superBinary);
		final byte[] geometryBinary = new byte[bytes.length - superLength - 4];
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
