package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameter;

public class VectorOverrideCommandLineOptions
{
	@Parameter(names = "--vectorstore", description = "By ingesting as both vectors and rasters you may want to ingest into different stores.  This will override the store for vector output.")
	private String vectorStore;
	@Parameter(names = "--vectorindex", description = "By ingesting as both vectors and rasters you may want each indexed differently.  This will override the index used for vector output.")
	private String vectorIndex;

	public VectorOverrideCommandLineOptions() {}

	public String getVectorStore() {
		return vectorStore;
	}

	public String getVectorIndex() {
		return vectorIndex;
	}
}
