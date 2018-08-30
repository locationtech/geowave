package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.MultiPolygon;

/**
 * Created by jwileczek on 7/20/18.
 */
public class MultiPolygonUDT extends
		AbstractGeometryUDT<MultiPolygon>
{
	@Override
	public Class<MultiPolygon> userClass() {
		return MultiPolygon.class;
	}
}
