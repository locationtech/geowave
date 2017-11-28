package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

@SuppressFBWarnings
public abstract class GeomFunction implements
		UDF2<String, String, Boolean>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeomFunction.class);
	protected final GeomReader geomReader = new GeomReader();

	protected Geometry parseGeom(
			String geomStr ) {
		try {
			return geomReader.read(geomStr);
		}
		catch (Exception e) {
			LOGGER.error(e.getMessage());
		}

		return null;
	}
}
