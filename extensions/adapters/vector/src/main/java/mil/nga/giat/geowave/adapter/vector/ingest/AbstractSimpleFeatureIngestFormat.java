package mil.nga.giat.geowave.adapter.vector.ingest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

abstract public class AbstractSimpleFeatureIngestFormat<I> implements
		IngestFormatPluginProviderSpi<I, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();
	protected final TypeNameOptionProvider typeNameOptionProvider = new TypeNameOptionProvider();
	protected final FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
	protected AbstractSimpleFeatureIngestPlugin<I> myInstance;

	private synchronized AbstractSimpleFeatureIngestPlugin<I> getInstance() {
		if (myInstance == null) {
			myInstance = newPluginInstance();
			myInstance.setFilterProvider(cqlFilterOptionProvider);
			myInstance.setTypeNameProvider(typeNameOptionProvider);
			myInstance.setSerializationFormatProvider(serializationFormatOptionProvider);
			setPluginInstanceOptionProviders();
		}
		return myInstance;
	}

	abstract protected AbstractSimpleFeatureIngestPlugin<I> newPluginInstance();

	protected void setPluginInstanceOptionProviders() {}

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
				getIngestFormatOptionProviders().toArray(
						new IngestFormatOptionProvider[] {}));
	}

	private List<IngestFormatOptionProvider> getIngestFormatOptionProviders() {
		// TODO: because other formats are not yet implemented,
		// don't expose the options to the user
		final List<IngestFormatOptionProvider> providers = new ArrayList<IngestFormatOptionProvider>();
		providers.add(serializationFormatOptionProvider);
		providers.add(cqlFilterOptionProvider);
		providers.add(typeNameOptionProvider);
		providers.addAll(internalGetIngestFormatOptionProviders());

		return providers;
	}

	protected Collection<? extends IngestFormatOptionProvider> internalGetIngestFormatOptionProviders() {
		return new ArrayList<IngestFormatOptionProvider>();
	}

}
