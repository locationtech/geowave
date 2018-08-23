package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.Point;

/**
 * Created by jwileczek on 7/20/18.
 */
public class PointUDT extends
		AbstractGeometryUDT<Point>
{

	@Override
	public Class<Point> userClass() {
		return Point.class;
	}
}
