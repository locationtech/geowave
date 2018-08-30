package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.LineString;

/**
 * Created by jwileczek on 7/20/18.
 */
public class LineStringUDT extends
		AbstractGeometryUDT<LineString>
{
	@Override
	public Class<LineString> userClass() {
		return LineString.class;
	}
}
