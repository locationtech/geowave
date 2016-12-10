package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

/**
 * This class can be used to modify the behavior of the Accumulo Data Store.
 *
 */
public class AccumuloOptions extends
		BaseDataStoreOptions
{
	@Parameter(names = "--useLocalityGroups", hidden = true, arity = 1)
	protected boolean useLocalityGroups = true;

	public boolean isUseLocalityGroups() {
		return useLocalityGroups;
	}

	public void setUseLocalityGroups(
			final boolean useLocalityGroups ) {
		this.useLocalityGroups = useLocalityGroups;
	}
}
