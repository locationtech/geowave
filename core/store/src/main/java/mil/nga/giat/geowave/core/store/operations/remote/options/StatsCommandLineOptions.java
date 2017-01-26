package mil.nga.giat.geowave.core.store.operations.remote.options;

import com.beust.jcommander.Parameter;

public class StatsCommandLineOptions
{

	public StatsCommandLineOptions() {}

	@Parameter(names = "--auth", description = "The authorizations used for the statistics calculation as a subset of the accumulo user authorization; by default all authorizations are used.")
	private String authorizations;

	@Parameter(names = "--json", description = "Output in JSON format.")
	private boolean jsonFormatFlag;

	public String getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			String authorizations ) {
		this.authorizations = authorizations;
	}

	public boolean getJsonFormatFlag() {
		return this.jsonFormatFlag;
	}

	public void setJsonFormatFlag(
			boolean jsonFormatFlag ) {
		this.jsonFormatFlag = jsonFormatFlag;
	}
}
