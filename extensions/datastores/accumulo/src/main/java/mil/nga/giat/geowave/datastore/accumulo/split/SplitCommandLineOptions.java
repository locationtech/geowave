package mil.nga.giat.geowave.datastore.accumulo.split;

import com.beust.jcommander.Parameter;

public class SplitCommandLineOptions
{
	@Parameter(names = "--indexId", description = "The geowave index ID (optional; default is all indices)")
	private String indexId;

	@Parameter(names = "--num", required = true, description = "The number of partitions (or entries)")
	private long number;

	public String getIndexId() {
		return indexId;
	}

	public long getNumber() {
		return number;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public void setNumber(
			long number ) {
		this.number = number;
	}
}
