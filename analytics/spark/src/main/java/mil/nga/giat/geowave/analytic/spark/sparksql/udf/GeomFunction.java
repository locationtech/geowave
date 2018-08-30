package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.BufferOperation;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

@SuppressFBWarnings
public abstract class GeomFunction implements
		UDF2<Geometry, Geometry, Boolean>,
		BufferOperation
{
	private GeomReader geomReader = new GeomReader();

	// Base GeomFunction will assume same bucket comparison
	public double getBufferAmount() {
		return 0.0;
	}

	@Override
	public Boolean call(
			Geometry t1,
			Geometry t2 )
			throws Exception {
		return apply(
				t1,
				t2);
	}

	public abstract boolean apply(
			Geometry geom1,
			Geometry geom2 );

	public String getRegisterName() {
		return this.getClass().getSimpleName();
	}
}
