package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import com.beust.jcommander.Parameter;

public class ComparisonCommandLineOptions
{
	@Parameter(names = "--timeAttribute", description = "The name of the time attribute")
	private String timeAttribute;

	public ComparisonCommandLineOptions() {

	}

	public ComparisonCommandLineOptions(
			final String timeAttribute ) {
		this.timeAttribute = timeAttribute;
	}

	public String getTimeAttribute() {
		return timeAttribute;
	}
}
