package mil.nga.giat.geowave.raster.resize;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableOutputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeMapper extends
		GeoWaveWritableOutputMapper<GeoWaveInputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;

	@Override
	protected void mapNativeValue(
			final GeoWaveInputKey key,
			final GridCoverage value,
			final MapContext<GeoWaveInputKey, GridCoverage, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		if (helper.isOriginalCoverage(key.getAdapterId())) {
			final DataAdapter<?> adapter = adapterStore.getAdapter(key.getAdapterId());
			if ((adapter != null) && (adapter instanceof RasterDataAdapter)) {
				final Iterator<GridCoverage> coverages = helper.getCoveragesForIndex(value);
				while (coverages.hasNext()) {
					final GridCoverage c = coverages.next();
					// it should be a FitToIndexGridCoverage because it was just
					// converted above
					if (c instanceof FitToIndexGridCoverage) {
						context.write(
								new GeoWaveInputKey(
										helper.getNewCoverageId(),
										((FitToIndexGridCoverage) c).getInsertionId()),
								c);
					}
				}
			}
		}
	}

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, GridCoverage, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
