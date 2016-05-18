package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;

public class Landsat8IngestCommandLineOptions
{
	@Parameter(names = "histogram", description = "An option to store the histogram of the values of the coverage so that histogram equalization will be performed")
	private boolean createHistogram;
	@Parameter(names = "pyramid", description = "An option to store an image pyramid for the coverage")
	private boolean createPyramid;
	@Parameter(names = "--retainimages", description = "An option to keep the images that are ingested in the local workspace directory.  By default it will delete the local file after it is ingested successfully.")
	private boolean retainImages;
	@Parameter(names = "--tilesize", description = "The option to set the pixel size for each tile stored in GeoWave. The default is " + RasterDataAdapter.DEFAULT_TILE_SIZE)
	private int tileSize;
	@Parameter(names = "--coverage", description = "The name to give to each unique coverage. Freemarker templating can be used for variable substition based on the same attributes used for filtering.  The default coverage name is '${entityId}'")
	private String coverageName;
	@Parameter(names = "--converter", description = "Prior to ingesting an image, this converter will be used to massage the data. The default is not to convert the data.")
	private String coverageConverter;

	public Landsat8IngestCommandLineOptions() {}

	public boolean isCreateHistogram() {
		return createHistogram;
	}

	public boolean isCreatePyramid() {
		return createPyramid;
	}

	public boolean isRetainImages() {
		return retainImages;
	}

	public String getCoverageName() {
		return coverageName;
	}

	public int getTileSize() {
		return tileSize;
	}
}
