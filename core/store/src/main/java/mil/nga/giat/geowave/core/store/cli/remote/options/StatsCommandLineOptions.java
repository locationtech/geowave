package mil.nga.giat.geowave.core.store.cli.remote.options;

import com.beust.jcommander.Parameter;

public class StatsCommandLineOptions
{

	@Parameter(names = "--auth", description = "The authorizations used for the statistics calculation as a subset of the accumulo user authorization; by default all authorizations are used.")
	private String authorizations;

	public StatsCommandLineOptions() {}

	public String getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			String authorizations ) {
		this.authorizations = authorizations;
	}
}
