package mil.nga.giat.geowave.core.store;

public interface GenericStoreFactory<T> extends
		GenericFactory
{
	/**
	 * Create the store, w/the options instance that was returned and populated
	 * w/createOptionsInstance().
	 */
	T createStore(
			StoreFactoryOptions options );

	/**
	 * An object used to configure the specific store. This really exists so
	 * that the command line options for JCommander can be filled in without
	 * knowing which options class we specifically have to create.
	 */
	StoreFactoryOptions createOptionsInstance();

}
