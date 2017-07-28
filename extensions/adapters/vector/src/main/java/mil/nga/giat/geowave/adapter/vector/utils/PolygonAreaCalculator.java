package mil.nga.giat.geowave.adapter.vector.utils;

import java.util.HashMap;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.densify.Densifier;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class PolygonAreaCalculator
{
	private static final double DEFAULT_DENSIFY_VERTEX_COUNT = 1000.0;
	private static final double SQM_2_SQKM = 1.0 / 1000000.0;
	private double densifyVertexCount = DEFAULT_DENSIFY_VERTEX_COUNT;

	private HashMap<String, CoordinateReferenceSystem> crsMap = new HashMap<>();

	public PolygonAreaCalculator() {}

	private CoordinateReferenceSystem lookupUtmCrs(
			double centerLat,
			double centerLon )
			throws NoSuchAuthorityCodeException,
			FactoryException {
		int epsgCode = 32700 - Math.round((45f + (float) centerLat) / 90f) * 100
				+ Math.round((183f + (float) centerLon) / 6f);

		String crsId = "EPSG:" + Integer.toString(epsgCode);

		CoordinateReferenceSystem crs = crsMap.get(crsId);

		if (crs == null) {
			crs = CRS.decode(
					crsId,
					true);

			crsMap.put(
					crsId,
					crs);
		}

		return crs;
	}

	public double getAreaSimple(
			Geometry polygon )
			throws Exception {
		Point centroid = polygon.getCentroid();
		CoordinateReferenceSystem equalAreaCRS = lookupUtmCrs(
				centroid.getY(),
				centroid.getX());

		MathTransform transform = CRS.findMathTransform(
				DefaultGeographicCRS.WGS84,
				equalAreaCRS,
				true);

		Geometry transformedPolygon = JTS.transform(
				polygon,
				transform);

		return transformedPolygon.getArea() * SQM_2_SQKM;
	}

	public double getAreaDensify(
			Geometry polygon )
			throws Exception {
		Point centroid = polygon.getCentroid();
		CoordinateReferenceSystem equalAreaCRS = lookupUtmCrs(
				centroid.getY(),
				centroid.getX());

		double vertexSpacing = polygon.getLength() / densifyVertexCount;
		Geometry densePolygon = Densifier.densify(
				polygon,
				vertexSpacing);

		MathTransform transform = CRS.findMathTransform(
				DefaultGeographicCRS.WGS84,
				equalAreaCRS,
				true);

		Geometry transformedPolygon = JTS.transform(
				densePolygon,
				transform);

		return transformedPolygon.getArea() * SQM_2_SQKM;
	}

	public void setDensifyVertexCount(
			double densifyVertexCount ) {
		this.densifyVertexCount = densifyVertexCount;
	}
}