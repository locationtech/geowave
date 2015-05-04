package mil.nga.giat.geowave.core.ingest;

/**
 * This interface can be injected using SPI to determine which supported index
 * for an ingest type will be used.
 * 
 */
public interface IngestDimensionalityTypeProviderSpi
{
	/**
	 * get a visitor than can assess the compatibility of an index
	 * 
	 * @return a visitor than can assess the compatibility of any supported
	 *         index with this dimensionality type. It is best for this visitor
	 *         to very specifically identify only indices that well match this
	 *         dimensionality type - for example, if two dimensionality types
	 *         exist, spatial and spatial-temporal, it is best if the spatial
	 *         dimensionality type is not over-selective if the existence of a
	 *         time dimension would better warrant using a spatial-temporal
	 *         index.
	 */
	public IndexCompatibilityVisitor getCompatibilityVisitor();

	/**
	 * This will represent the name for the dimensionality type that is
	 * registered with the ingest framework and presented as a dimensionality
	 * type option via the commandline. For consistency, this name is preferably
	 * lower-case and without spaces, and should uniquely identify the
	 * dimensionality type as much as possible.
	 * 
	 * @return
	 */
	public String getDimensionalityTypeName();

	/**
	 * if the registered dimensionality types are listed by a user, this can
	 * provide a user-friendly description for each
	 * 
	 * @return the user-friendly description
	 */
	public String getDimensionalityTypeDescription();

	/**
	 * If there are multiple acceptable dimensionality types, the one with the
	 * highest priority will be used
	 * 
	 * @return a priority value, any integer will work, it is merely important
	 *         to consider values relative to each other
	 */
	public int getPriority();
}
