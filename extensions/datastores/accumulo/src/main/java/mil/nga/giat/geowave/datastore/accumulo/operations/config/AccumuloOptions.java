package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import com.beust.jcommander.Parameter;

/**
 * This class can be used to modify the behavior of the Accumulo Data Store.
 * 
 */
public class AccumuloOptions
{
	@Parameter(names = "--persistAdapter", hidden = true, arity = 1)
	protected boolean persistAdapter = true;

	@Parameter(names = "--persistIndex", hidden = true, arity = 1)
	protected boolean persistIndex = true;

	@Parameter(names = "--persistDataStatistics", hidden = true, arity = 1)
	protected boolean persistDataStatistics = true;

	@Parameter(names = "--createTable", hidden = true, arity = 1)
	protected boolean createTable = true;

	@Parameter(names = "--useLocalityGroups", hidden = true, arity = 1)
	protected boolean useLocalityGroups = true;

	@Parameter(names = "--useAltIndex", hidden = true, arity = 1)
	protected boolean useAltIndex = false;

	@Parameter(names = "--enableBlockCache", hidden = true, arity = 1)
	protected boolean enableBlockCache = true;

	public boolean isPersistDataStatistics() {
		return persistDataStatistics;
	}

	public void setPersistDataStatistics(
			final boolean persistDataStatistics ) {
		this.persistDataStatistics = persistDataStatistics;
	}

	public boolean isPersistAdapter() {
		return persistAdapter;
	}

	public void setPersistAdapter(
			final boolean persistAdapter ) {
		this.persistAdapter = persistAdapter;
	}

	public boolean isPersistIndex() {
		return persistIndex;
	}

	public void setPersistIndex(
			final boolean persistIndex ) {
		this.persistIndex = persistIndex;
	}

	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(
			final boolean createTable ) {
		this.createTable = createTable;
	}

	public boolean isUseLocalityGroups() {
		return useLocalityGroups;
	}

	public void setUseLocalityGroups(
			final boolean useLocalityGroups ) {
		this.useLocalityGroups = useLocalityGroups;
	}

	public boolean isUseAltIndex() {
		return useAltIndex;
	}

	public void setUseAltIndex(
			final boolean useAltIndex ) {
		this.useAltIndex = useAltIndex;
	}

	public boolean isEnableBlockCache() {
		return enableBlockCache;
	}

	public void setEnableBlockCache(
			boolean enableBlockCache ) {
		this.enableBlockCache = enableBlockCache;
	}
}
