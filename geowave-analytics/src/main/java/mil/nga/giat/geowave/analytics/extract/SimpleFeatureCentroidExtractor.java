package mil.nga.giat.geowave.analytics.extract;

import java.util.Iterator;

import mil.nga.giat.geowave.vector.adapter.FeatureGeometryHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.ReferenceIdentifier;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * Extract a set of points representing critical points for a simple feature
 * that me be representative or compared to centroids.
 * 
 */
public class SimpleFeatureCentroidExtractor implements
		CentroidExtractor<SimpleFeature>
{
	@Override
	public Point getCentroid(
			final SimpleFeature anObject ) {
		final FeatureGeometryHandler handler = new FeatureGeometryHandler(
				anObject.getDefaultGeometryProperty().getDescriptor());
		final Geometry geometry = handler.toIndexValue(
				anObject).getGeometry();
		final int srid = getSRID(anObject);
		final Point point = geometry.getCentroid();
		point.setSRID(srid);
		return point;
	}

	protected int getSRID(
			final SimpleFeature geometryFeature ) {
		final ReferenceIdentifier id = getFirst(geometryFeature.getDefaultGeometryProperty().getDescriptor().getCoordinateReferenceSystem().getIdentifiers());
		if (id == null) {
			return 4326;
		}
		return Integer.parseInt(id.getCode());
	}

	private static final <T> ReferenceIdentifier getFirst(
			final Iterable<ReferenceIdentifier> iterable ) {
		final Iterator<ReferenceIdentifier> it = iterable.iterator();
		if (it.hasNext()) {
			final ReferenceIdentifier id = it.next();
			if ("EPSG".equals(id.getCodeSpace())) {
				return id;
			}
		}
		return null;
	}

}
