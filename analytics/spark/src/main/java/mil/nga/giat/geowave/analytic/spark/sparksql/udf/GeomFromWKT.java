package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by jwileczek on 8/16/18.
 */
public class GeomFromWKT implements
		UDF1<String, Geometry>
{

	@Override
	public Geometry call(
			String o )
			throws Exception {
		return new WKTReader().read(o);
	}

}
