package mil.nga.giat.geowave.analytic.distance;

import java.io.Serializable;

/**
 * Determine the distance between two objects.
 * 
 * @param <T>
 */
public interface DistanceFn<T> extends
		Serializable
{
	double measure(
			T x,
			T y );
}
