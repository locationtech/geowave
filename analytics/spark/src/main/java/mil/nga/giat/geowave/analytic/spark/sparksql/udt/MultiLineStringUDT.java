package mil.nga.giat.geowave.analytic.spark.sparksql.udt;

import com.vividsolutions.jts.geom.MultiLineString;

/**
 * Created by jwileczek on 7/20/18.
 */
public class MultiLineStringUDT extends
		AbstractGeometryUDT<MultiLineString>
{

	@Override
	public Class<MultiLineString> userClass() {
		return MultiLineString.class;
	}

}
