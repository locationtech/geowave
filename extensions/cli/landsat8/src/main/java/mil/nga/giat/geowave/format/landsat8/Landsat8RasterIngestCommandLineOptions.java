package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IntegerConverter;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;

public class Landsat8RasterIngestCommandLineOptions
{
	@Parameter(names = "--histogram", description = "An option to store the histogram of the values of the coverage so that histogram equalization will be performed")
	private boolean createHistogram = false;
	@Parameter(names = "--pyramid", description = "An option to store an image pyramid for the coverage")
	private boolean createPyramid = false;
	@Parameter(names = "--retainimages", description = "An option to keep the images that are ingested in the local workspace directory.  By default it will delete the local file after it is ingested successfully.")
	private boolean retainImages = false;
	@Parameter(names = "--tilesize", description = "The option to set the pixel size for each tile stored in GeoWave. The default is "
			+ RasterDataAdapter.DEFAULT_TILE_SIZE)
	private int tileSize = 512;
	@Parameter(names = "--coverage", description = "The name to give to each unique coverage. Freemarker templating can be used for variable substition based on the same attributes used for filtering.  The default coverage name is '${"
			+ SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME
			+ "}_${"
			+ BandFeatureIterator.BAND_ATTRIBUTE_NAME
			+ "}'.  If ${band} is unused in the coverage name, all bands will be merged together into the same coverage.")
	private String coverageName = "${" + SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME + "}_${"
			+ BandFeatureIterator.BAND_ATTRIBUTE_NAME + "}";

	@Parameter(names = "--converter", description = "Prior to ingesting an image, this converter will be used to massage the data. The default is not to convert the data.")
	private String coverageConverter;
	@Parameter(names = "--subsample", description = "Subsample the image prior to ingest by the scale factor provided.  The scale factor should be an integer value greater than 1.", converter = IntegerConverter.class)
	private int scale = 1;
	@Parameter(names = "--crop", description = "Use the spatial constraint provided in CQL to crop the image.  If no spatial constraint is provided, this will not have an effect.")
	private boolean cropToSpatialConstraint;
	@Parameter(names = "--skipMerge", description = "By default the ingest will automerge overlapping tiles as a post-processing optimization step for efficient retrieval, but this will skip the merge process")
	private boolean skipMerge;

	public Landsat8RasterIngestCommandLineOptions() {}

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

	public String getCoverageConverter() {
		return coverageConverter;
	}

	public boolean isCoveragePerBand() {
		// technically the coverage will be per band if it contains any of the
		// band attribute names, but realistically the band name should be the
		// only one used
		return coverageName.contains("${" + BandFeatureIterator.BAND_ATTRIBUTE_NAME + "}")
				|| coverageName.contains("${" + BandFeatureIterator.BAND_DOWNLOAD_ATTRIBUTE_NAME + "}")
				|| coverageName.contains("${" + BandFeatureIterator.SIZE_ATTRIBUTE_NAME + "}");

	}

	public int getTileSize() {
		return tileSize;
	}

	public boolean isSubsample() {
		return (scale > 1);
	}

	public int getScale() {
		return scale;
	}

	public boolean isCropToSpatialConstraint() {
		return cropToSpatialConstraint;
	}

	public void setCreateHistogram(
			final boolean createHistogram ) {
		this.createHistogram = createHistogram;
	}

	public void setCreatePyramid(
			final boolean createPyramid ) {
		this.createPyramid = createPyramid;
	}

	public void setRetainImages(
			final boolean retainImages ) {
		this.retainImages = retainImages;
	}

	public void setTileSize(
			final int tileSize ) {
		this.tileSize = tileSize;
	}

	public void setCoverageName(
			final String coverageName ) {
		this.coverageName = coverageName;
	}

	public void setCoverageConverter(
			final String coverageConverter ) {
		this.coverageConverter = coverageConverter;
	}

	public void setScale(
			final int scale ) {
		this.scale = scale;
	}

	public void setCropToSpatialConstraint(
			final boolean cropToSpatialConstraint ) {
		this.cropToSpatialConstraint = cropToSpatialConstraint;
	}

	public boolean isSkipMerge() {
		return skipMerge;
	}

	public void setSkipMerge(
			boolean skipMerge ) {
		this.skipMerge = skipMerge;
	}
}
