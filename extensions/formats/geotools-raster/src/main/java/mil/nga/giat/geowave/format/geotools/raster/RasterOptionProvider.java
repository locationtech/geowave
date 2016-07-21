package mil.nga.giat.geowave.format.geotools.raster;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.DoubleConverter;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class RasterOptionProvider implements
		IngestFormatOptionProvider
{

	@Parameter(names = "--pyramid", description = "Build an image pyramid on ingest for quick reduced resolution query")
	private boolean buildPyramid = false;

	@Parameter(names = "--crs", description = "A CRS override for the provided raster file")
	private String crs = null;
	
	@Parameter(names = "--histogram", description = "Build a histogram of samples per band on ingest for performing band equalization")
	private boolean buildHistogream = false;

	@Parameter(names = "--tileSize", description = "Optional parameter to set the tile size stored (default is 256)")
	private int tileSize = RasterDataAdapter.DEFAULT_TILE_SIZE;

	@Parameter(names = "--coverage", description = "Optional parameter to set the coverage name (default is the file name)")
	private String coverageName = null;

	@Parameter(names = "--nodata", variableArity = true, description = "Optional parameter to set 'no data' values, if 1 value is giving it is applied for each band, if multiple are given then the first totalNoDataValues/totalBands are applied to the first band and so on, so each band can have multiple differing 'no data' values if needed", converter = DoubleConverter.class)
	private List<Double> nodata = new ArrayList<>();

	@Parameter(names = "--separateBands", description = "Optional parameter to separate each band into its own coverage name. By default the coverage name will have '_Bn' appended to it where `n` is the band's index.")
	private boolean separateBands = false;

	public RasterOptionProvider() {}

	public boolean isBuildPyramid() {
		return buildPyramid;
	}

	public int getTileSize() {
		return tileSize;
	}

	public boolean isSeparateBands() {
		return separateBands;
	}

	public String getCrs() {
		return crs;
	}

	public String getCoverageName() {
		if ((coverageName == null) || coverageName.trim().isEmpty()) {
			return null;
		}
		return coverageName;
	}

	public boolean isBuildHistogream() {
		return buildHistogream;
	}

	public double[][] getNodata(
			final int numBands ) {
		if (nodata.isEmpty() || (numBands <= 0)) {
			return null;
		}
		final double[][] retVal = new double[numBands][];
		final int nodataPerBand = nodata.size() / numBands;
		if (nodataPerBand <= 1) {
			for (int b = 0; b < numBands; b++) {
				retVal[b] = new double[] {
					nodata.get(Math.min(
							b,
							nodata.size() - 1))
				};
			}
		}
		else {
			for (int b = 0; b < retVal.length; b++) {
				retVal[b] = new double[nodataPerBand];
				for (int i = 0; i < nodataPerBand; i++) {
					retVal[b][i] = nodata.get((b * nodataPerBand) + i);
				}
			}
		}
		return retVal;
	}
}
