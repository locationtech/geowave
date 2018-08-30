package mil.nga.giat.geowave.analytic.spark.sparksql;

import com.vividsolutions.jts.geom.*;
import mil.nga.giat.geowave.analytic.spark.sparksql.udt.*;
import org.apache.spark.sql.types.UDTRegistration;

/**
 * Created by jwileczek on 7/24/18.
 */
public class GeoWaveSpatialEncoders
{

	public static GeometryUDT geometryUDT = new GeometryUDT();
	public static PointUDT pointUDT = new PointUDT();
	public static LineStringUDT lineStringUDT = new LineStringUDT();
	public static PolygonUDT polygonUDT = new PolygonUDT();
	public static MultiPointUDT multiPointUDT = new MultiPointUDT();
	public static MultiPolygonUDT multiPolygonUDT = new MultiPolygonUDT();

	public static void registerUDTs() {
		UDTRegistration.register(
				Geometry.class.getCanonicalName(),
				GeometryUDT.class.getCanonicalName());
		UDTRegistration.register(
				Point.class.getCanonicalName(),
				PointUDT.class.getCanonicalName());
		UDTRegistration.register(
				LineString.class.getCanonicalName(),
				LineStringUDT.class.getCanonicalName());
		UDTRegistration.register(
				Polygon.class.getCanonicalName(),
				PolygonUDT.class.getCanonicalName());

		UDTRegistration.register(
				MultiLineString.class.getCanonicalName(),
				MultiLineStringUDT.class.getCanonicalName());
		UDTRegistration.register(
				MultiPoint.class.getCanonicalName(),
				MultiPointUDT.class.getCanonicalName());
		UDTRegistration.register(
				MultiPolygon.class.getCanonicalName(),
				MultiPolygonUDT.class.getCanonicalName());

	}
}
