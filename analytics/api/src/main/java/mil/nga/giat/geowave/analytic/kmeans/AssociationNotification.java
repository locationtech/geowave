package mil.nga.giat.geowave.analytic.kmeans;

import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;

/**
 * 
 * Callback with the pairing of a point to its closest centroid at a zoom level.
 * 
 * @see CentroidAssociationFn
 * @param <T>
 */
public interface AssociationNotification<T>
{
	public void notify(
			CentroidPairing<T> pairing );
}
