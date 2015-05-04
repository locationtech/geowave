package mil.nga.giat.geowave.analytic.distance;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureCentroidOrthodromicDistanceFn extends
		FeatureCentroidDistanceFn implements
		DistanceFn<SimpleFeature>
{

	private static final long serialVersionUID = -9077135292765517738L;

	public FeatureCentroidOrthodromicDistanceFn() {
		super(
				new CoordinateCircleDistanceFn());
	}

}
