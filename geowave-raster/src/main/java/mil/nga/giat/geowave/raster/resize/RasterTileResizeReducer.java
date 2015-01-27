package mil.nga.giat.geowave.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					helper.getGeoWaveOutputKey(),
					mergedCoverage);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
