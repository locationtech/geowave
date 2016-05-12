package mil.nga.giat.geowave.adapter.vector.ingest;

import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

/**
 * This class is a holder class for options used in AbstractSimpleFeatureIngest.
 */
public class SimpleFeatureIngestOptions implements
		IngestFormatOptionProvider
{

	@ParametersDelegate
	private CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

	@ParametersDelegate
	private TypeNameOptionProvider typeNameOptionProvider = new TypeNameOptionProvider();

	@ParametersDelegate
	private FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();

	@ParametersDelegate
	private Object pluginOptions = null;

	public SimpleFeatureIngestOptions() {}

	public CQLFilterOptionProvider getCqlFilterOptionProvider() {
		return cqlFilterOptionProvider;
	}

	public void setCqlFilterOptionProvider(
			CQLFilterOptionProvider cqlFilterOptionProvider ) {
		this.cqlFilterOptionProvider = cqlFilterOptionProvider;
	}

	public TypeNameOptionProvider getTypeNameOptionProvider() {
		return typeNameOptionProvider;
	}

	public void setTypeNameOptionProvider(
			TypeNameOptionProvider typeNameOptionProvider ) {
		this.typeNameOptionProvider = typeNameOptionProvider;
	}

	public FeatureSerializationOptionProvider getSerializationFormatOptionProvider() {
		return serializationFormatOptionProvider;
	}

	public void setSerializationFormatOptionProvider(
			FeatureSerializationOptionProvider serializationFormatOptionProvider ) {
		this.serializationFormatOptionProvider = serializationFormatOptionProvider;
	}

	public Object getPluginOptions() {
		return pluginOptions;
	}

	public void setPluginOptions(
			Object pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}
