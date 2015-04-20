package mil.nga.giat.geowave.datastore.accumulo;

/**
 * This class can be used to modify the behavior of the Accumulo Data Store.
 * 
 */
public class AccumuloOptions
{
	protected boolean persistAdapter = true;
	protected boolean persistIndex = true;
	protected boolean persistDataStatistics = true;
	protected boolean createTable = true;
	protected boolean useLocalityGroups = true;
	protected boolean useAltIndex = true;

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
}
