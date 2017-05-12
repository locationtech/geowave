package mil.nga.giat.geowave.adapter.vector.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * A collection of statistics configurations targeted to a specific attribute.
 * Each configuration describes how to construct a statistic for an attribute.
 * 
 */
public class StatsConfigurationCollection implements
		java.io.Serializable
{

	private static final long serialVersionUID = -4983543525776889248L;

	private final static Logger LOGGER = LoggerFactory.getLogger(StatsConfigurationCollection.class);

	private List<StatsConfig<SimpleFeature>> configurationsForAttribute;

	public StatsConfigurationCollection() {

	}

	public StatsConfigurationCollection(
			final List<StatsConfig<SimpleFeature>> configurationsForAttribute ) {
		this.configurationsForAttribute = configurationsForAttribute;
	}

	public List<StatsConfig<SimpleFeature>> getConfigurationsForAttribute() {
		return configurationsForAttribute;
	}

	public void setConfigurationsForAttribute(
			final List<StatsConfig<SimpleFeature>> configrationsForAttribute ) {
		this.configurationsForAttribute = configrationsForAttribute;
	}

	public static class SimpleFeatureStatsConfigurationCollection implements
			SimpleFeatureUserDataConfiguration
	{

		private static final long serialVersionUID = -9149753182284018327L;
		private Map<String, StatsConfigurationCollection> attConfig = new HashMap<String, StatsConfigurationCollection>();

		public SimpleFeatureStatsConfigurationCollection() {}

		public SimpleFeatureStatsConfigurationCollection(
				final SimpleFeatureType type ) {
			super();
			configureFromType(type);
		}

		public Map<String, StatsConfigurationCollection> getAttConfig() {
			return attConfig;
		}

		public void setAttConfig(
				final Map<String, StatsConfigurationCollection> attConfig ) {
			this.attConfig = attConfig;
		}

		@Override
		public void updateType(
				final SimpleFeatureType type ) {
			for (final Map.Entry<String, StatsConfigurationCollection> item : attConfig.entrySet()) {
				final AttributeDescriptor desc = type.getDescriptor(item.getKey());
				if (desc == null) {
					LOGGER.error("Attribute " + item.getKey() + " not found for statistics configuration");
					continue;
				}
				desc.getUserData().put(
						"stats",
						item.getValue());
			}
		}

		@Override
		public void configureFromType(
				final SimpleFeatureType type ) {
			for (final AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
				if (descriptor.getUserData().containsKey(
						"stats")) {
					final Object configObj = descriptor.getUserData().get(
							"stats");
					if (!(configObj instanceof StatsConfigurationCollection)) {
						LOGGER.error("Invalid entry stats entry for " + descriptor.getLocalName());
						continue;
					}
					attConfig.put(
							descriptor.getLocalName(),
							(StatsConfigurationCollection) configObj);
				}
			}
		}
	}
}
