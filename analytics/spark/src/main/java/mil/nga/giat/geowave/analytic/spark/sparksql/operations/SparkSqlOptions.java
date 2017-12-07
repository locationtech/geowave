package mil.nga.giat.geowave.analytic.spark.sparksql.operations;

import com.beust.jcommander.Parameter;

public class SparkSqlOptions
{
	@Parameter(names = {
		"--csv"
	}, description = "The output CSV file name")
	private String csvOutputFile = null;

	@Parameter(names = {
		"--out"
	}, description = "The output datastore name")
	private String outputStoreName = null;

	@Parameter(names = {
		"--outtype"
	}, description = "The output feature type (adapter) name")
	private String outputTypeName = null;

	@Parameter(names = {
		"-s",
		"--show"
	}, description = "Number of result rows to display")
	private int showResults = 20;

	public SparkSqlOptions() {}

	public String getOutputStoreName() {
		return outputStoreName;
	}

	public void setOutputStoreName(
			String outputStoreName ) {
		this.outputStoreName = outputStoreName;
	}

	public int getShowResults() {
		return showResults;
	}

	public void setShowResults(
			int showResults ) {
		this.showResults = showResults;
	}

	public String getOutputTypeName() {
		return outputTypeName;
	}

	public void setOutputTypeName(
			String outputTypeName ) {
		this.outputTypeName = outputTypeName;
	}

	public String getCsvOutputFile() {
		return csvOutputFile;
	}

	public void setCsvOutputFile(
			String csvOutputFile ) {
		this.csvOutputFile = csvOutputFile;
	}
}
