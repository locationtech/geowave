package mil.nga.giat.geowave.analytics.kmeans;

import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.CentroidPairing;

/**
 * 
 * Compute the distance of a set of points to their closest centroid, providing
 * the resulting total sum. The value can be used to evaluate the quality of a
 * set of centroids for a given set of points, such that higher value indicates
 * lower quality centroids. The value can be use to evaluate the probability of
 * a point not being part associated with a set of centroids, where the higher
 * score indicates the less likely a point is a fit to a set of centroids.
 * 
 * Phi(C) where C is a set of centroids. As documented in section 3.1 in
 * 
 * Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB
 * Endowment Vol. 5, No. 7. 2012.
 * 
 * 
 */
public class CentroidAssociationFn<T>
{
	private DistanceFn<T> distanceFunction;

	public DistanceFn<T> getDistanceFunction() {
		return distanceFunction;
	}

	public void setDistanceFunction(
			final DistanceFn<T> distanceFunction ) {
		this.distanceFunction = distanceFunction;
	}

	public double compute(
			final AnalyticItemWrapper<T> point,
			final Iterable<AnalyticItemWrapper<T>> targetSet,
			final AssociationNotification<T> associationNotification ) {
		final CentroidPairing<T> pairing = new CentroidPairing<T>(
				null,
				point,
				Double.POSITIVE_INFINITY);
		for (final AnalyticItemWrapper<T> y : targetSet) {
			final double distance = distanceFunction.measure(
					point.getWrappedItem(),
					y.getWrappedItem());
			if (distance < pairing.getDistance()) {
				pairing.setDistance(distance);
				pairing.setCentroid(y);
			}
		}
		associationNotification.notify(pairing);
		return pairing.getDistance();
	}

	public double compute(
			final Iterable<AnalyticItemWrapper<T>> pointSet,
			final Iterable<AnalyticItemWrapper<T>> targetSet,
			final AssociationNotification<T> associationNotification ) {
		double sum = 0.0;
		for (final AnalyticItemWrapper<T> point : pointSet) {
			sum += this.compute(
					point,
					targetSet,
					associationNotification);
		}
		return sum;
	}
}
