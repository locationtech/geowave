package mil.nga.giat.geowave.format.geotools.vector;

import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.core.ingest.CompoundIngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.format.geotools.vector.retyping.date.DateFieldOptionProvider;
import mil.nga.giat.geowave.format.geotools.vector.retyping.date.DateFieldRetypingPlugin;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest format plugin provider for GeoTools vector data
 * stores. It currently only supports ingesting data directly from a local file
 * system into GeoWave.
 */
public class GeoToolsVectorDataStoreIngestFormat implements
		IngestFormatPluginProviderSpi<Object, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();
	protected final DateFieldOptionProvider dateFieldOptionProvider = new DateFieldOptionProvider();

	@Override
	public AvroFormatPlugin<Object, SimpleFeature> getAvroFormatPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested using intermediate avro files");
	}

	@Override
	public IngestFromHdfsPlugin<Object, SimpleFeature> getIngestFromHdfsPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return new GeoToolsVectorDataStoreIngestPlugin(
				new DateFieldRetypingPlugin(
						dateFieldOptionProvider),
				cqlFilterOptionProvider);
	}

	@Override
	public String getIngestFormatName() {
		return "geotools-vector";
	}

	@Override
	public String getIngestFormatDescription() {
		return "all file-based vector datastores supported within geotools";
	}

	@Override
	public IngestFormatOptionProvider getIngestFormatOptionProvider() {
		return new CompoundIngestFormatOptionProvider().add(
				cqlFilterOptionProvider).add(
				dateFieldOptionProvider);
	}

}
