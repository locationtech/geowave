package mil.nga.giat.geowave.adapter.vector.ingest;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;

abstract public class AbstractSimpleFeatureIngestFormat<I> implements
		IngestFormatPluginProviderSpi<I, SimpleFeature>
{
	protected final SimpleFeatureIngestOptions myOptions = new SimpleFeatureIngestOptions();

	private AbstractSimpleFeatureIngestPlugin<I> getInstance(
			IngestFormatOptionProvider options ) {
		AbstractSimpleFeatureIngestPlugin<I> myInstance = newPluginInstance(options);
		myInstance.setFilterProvider(myOptions.getCqlFilterOptionProvider());
		myInstance.setTypeNameProvider(myOptions.getTypeNameOptionProvider());
		myInstance.setSerializationFormatProvider(myOptions.getSerializationFormatOptionProvider());
		return myInstance;
	}

	abstract protected AbstractSimpleFeatureIngestPlugin<I> newPluginInstance(
			IngestFormatOptionProvider options );

	@Override
	public AvroFormatPlugin<I, SimpleFeature> createAvroFormatPlugin(
			IngestFormatOptionProvider options ) {
		return getInstance(options);
	}

	@Override
	public IngestFromHdfsPlugin<I, SimpleFeature> createIngestFromHdfsPlugin(
			IngestFormatOptionProvider options ) {
		return getInstance(options);
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> createLocalFileIngestPlugin(
			IngestFormatOptionProvider options ) {
		return getInstance(options);
	}

	/**
	 * Create an options instance. We may want to change this code from a
	 * singleton instance to actually allow multiple instances per format.
	 */
	@Override
	public IngestFormatOptionProvider createOptionsInstances() {
		myOptions.setPluginOptions(internalGetIngestFormatOptionProviders());
		return myOptions;
	}

	protected Object internalGetIngestFormatOptionProviders() {
		return null;
	}

}
