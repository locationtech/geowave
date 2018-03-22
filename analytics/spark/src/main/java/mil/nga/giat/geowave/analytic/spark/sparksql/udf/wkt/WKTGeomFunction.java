package mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.BufferOperation;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

@SuppressFBWarnings
public abstract class WKTGeomFunction implements
		UDF2<String, String, Boolean>,
		BufferOperation
{
	private GeomReader geomReader = new GeomReader();
	private GeomFunction function;

	public WKTGeomFunction(
			GeomFunction function ) {
		this.function = function;
	}

	// Base GeomFunction will assume same bucket comparison
	public double getBufferAmount() {
		return function.getBufferAmount();
	}

	@Override
	public Boolean call(
			String geomStringLeft,
			String geomStringRight )
			throws Exception {
		Geometry geom1 = parseGeom(geomStringLeft);
		Geometry geom2 = parseGeom(geomStringRight);

		if (geom1 == null || geom2 == null) {
			return false;
		}
		return function.apply(
				geom1,
				geom2);

	}

	public Geometry parseGeom(
			String wktString )
			throws ParseException {
		return geomReader.read(wktString);
	}
}
