package mil.nga.giat.geowave.test.kafka;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.format.gpx.GPXConsumer;
import mil.nga.giat.geowave.format.gpx.GpxIngestPlugin;
import mil.nga.giat.geowave.format.gpx.GpxTrack;

import org.opengis.feature.simple.SimpleFeature;

public class GpxIngestPluginUtil extends
		GpxIngestPlugin
{

	public GpxIngestPluginUtil() {
		super();
	}

	public GPXConsumer toGPXConsumer(
			final GpxTrack gpxTrack,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final GPXConsumer gpxConsumer = (GPXConsumer) toGeoWaveDataInternal(
				gpxTrack,
				primaryIndexId,
				globalVisibility);
		final GeoWaveData<SimpleFeature> data = gpxConsumer.next();
		if (data == null) {
			return null;
		}
		final Object defaultGeom = data.getValue().getDefaultGeometry();

		return gpxConsumer;
	}

}
