package mil.nga.giat.geowave.analytic.mapreduce.nn;

public interface DistanceProfileGenerateFn<CONTEXT, ITEM>
{
	/*
	 * Compute distance profile for given items.
	 */
	public DistanceProfile<CONTEXT> computeProfile(
			ITEM item1,
			ITEM item2 );
}
