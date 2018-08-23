package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.MultiPoint;

/**
 * Created by jwileczek on 7/20/18.
 */
public class MultiPointUDT extends
		AbstractGeometryUDT<MultiPoint>
{
	@Override
	public Class<MultiPoint> userClass() {
		return MultiPoint.class;
	}
}
