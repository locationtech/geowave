package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.Polygon;

/**
 * Created by jwileczek on 7/20/18.
 */
public class PolygonUDT extends
		AbstractGeometryUDT<Polygon>
{
	@Override
	public Class<Polygon> userClass() {
		return Polygon.class;
	}
}
