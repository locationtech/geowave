package mil.nga.giat.geowave.analytics.distance;

/**
 * Determine the distance between two objects.
 * 
 * @param <T>
 */
public interface DistanceFn<T>
{
	double measure(
			T x,
			T y );
}
