package mil.nga.giat.geowave.analytics.kmeans;

import mil.nga.giat.geowave.analytics.tools.CentroidPairing;

public interface AssociationNotification<T>
{
	public void notify(CentroidPairing<T> pairing);
}
