package mil.nga.giat.geowave.core.geotime.ingest;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.Bias;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.BiasConverter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.UnitConverter;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;

import com.beust.jcommander.Parameter;

public class SpatialTemporalOptions implements
		DimensionalityTypeOptions
{
	protected static Unit DEFAULT_PERIODICITY = Unit.YEAR;

	@Parameter(names = {
		"--period"
	}, required = false, description = "The periodicity of the temporal dimension.  Because time is continuous, it is binned at this interval.", converter = UnitConverter.class)
	protected Unit periodicity = DEFAULT_PERIODICITY;

	@Parameter(names = {
		"--bias"
	}, required = false, description = "The bias of the spatial-temporal index. There can be more precision given to time or space if necessary.", converter = BiasConverter.class)
	protected Bias bias = Bias.BALANCED;
	@Parameter(names = {
		"--maxDuplicates"
	}, required = false, description = "The max number of duplicates per dimension range.  The default is 2 per range (for example lines and polygon timestamp data would be up to 4 because its 2 dimensions, and line/poly time range data would be 8).")
	protected long maxDuplicates = -1;

	@Parameter(names = {
		"-c",
		"--crs"
	}, required = false, description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
	protected String crs = GeometryUtils.DEFAULT_CRS_STR;

	public void setCrs(
			String crs ) {
		this.crs = crs;
	}
}