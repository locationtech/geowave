package mil.nga.giat.geowave.core.store.cli.remote.options;

import com.beust.jcommander.Parameter;

public class VisibilityOptions
{
	@Parameter(names = {
		"-v",
		"--visibility"
	}, description = "The visibility of the data ingested (optional; default is 'public')")
	private String visibility;

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(
			String visibility ) {
		this.visibility = visibility;
	}
}
