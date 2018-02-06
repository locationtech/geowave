package mil.nga.giat.geowave.format.osm;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;

public class OSMIngestFormat extends
AbstractSimpleFeatureIngestFormat<WholeFile>
{

@Override
public String getIngestFormatName() {
return "osm";
}

@Override
public String getIngestFormatDescription() {
return "Ingest Raw format OSM data into geowave.";
}

@Override
protected AbstractSimpleFeatureIngestPlugin<WholeFile> newPluginInstance(
		IngestFormatOptionProvider options ) {
	// TODO Auto-generated method stub
	return new OSMIngestPlugin();
}

}