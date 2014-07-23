package mil.nga.giat.geowave.accumulo;

/**
 * This class can be used to modify the behavior of the Accumulo Data Store.
 * 
 */
public class AccumuloOptions
{
	protected boolean persistAdapter = true;
	protected boolean persistIndex = true;
	protected boolean createIndex = true;
	protected boolean useLocalityGroups = true;
	protected boolean useAltIndex = true;

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

	public boolean isCreateIndex() {
		return createIndex;
	}

	public void setCreateIndex(
			final boolean createIndex ) {
		this.createIndex = createIndex;
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
