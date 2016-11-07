package mil.nga.giat.geowave.format.twitter;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

/**
 * This represents an ingest format plugin provider for Twitter data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class TwitterIngestFormat extends
		AbstractSimpleFeatureIngestFormat<WholeFile>
{

	@Override
	protected AbstractSimpleFeatureIngestPlugin<WholeFile> newPluginInstance(
			final IngestFormatOptionProvider options ) {
		return new TwitterIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "twitter";
	}

	@Override
	public String getIngestFormatDescription() {
		return "Flattened compressed files from Twitter API";
	}
}
