package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

abstract public class AbstractSimpleFeatureIngestFormat<I> implements
		IngestFormatPluginProviderSpi<I, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();
	protected final FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
	protected AbstractSimpleFeatureIngestPlugin<I> myInstance;

	private synchronized AbstractSimpleFeatureIngestPlugin<I> getInstance() {
		if (myInstance == null) {
			myInstance = newPluginInstance();
			myInstance.setFilterProvider(cqlFilterOptionProvider);
			myInstance.setSerializationFormatProvider(serializationFormatOptionProvider);
		}
		return myInstance;
	}

	abstract protected AbstractSimpleFeatureIngestPlugin<I> newPluginInstance();

	@Override
	public AvroFormatPlugin<I, SimpleFeature> getAvroFormatPlugin() {
		return getInstance();
	}

	@Override
	public IngestFromHdfsPlugin<I, SimpleFeature> getIngestFromHdfsPlugin() {
		return getInstance();
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return getInstance();
	}

	@Override
	public IngestFormatOptionProvider getIngestFormatOptionProvider() {
		return new MultiOptionProvider(
				new IngestFormatOptionProvider[] {
					// TODO: because other formats are not yet implemented,
					// don't expose the options to the user
					serializationFormatOptionProvider,
					cqlFilterOptionProvider
				});
	}

}
