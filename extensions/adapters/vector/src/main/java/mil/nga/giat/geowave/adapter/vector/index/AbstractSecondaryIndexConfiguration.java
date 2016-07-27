package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Set;

import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

public abstract class AbstractSecondaryIndexConfiguration<T> implements
		SimpleFeatureSecondaryIndexConfiguration
{

	private static final long serialVersionUID = -7425830022998223202L;
	private final static Logger LOGGER = Logger.getLogger(AbstractSecondaryIndexConfiguration.class);
	private final Class<T> clazz;
	private final Set<String> attributes;
	private final SecondaryIndexType secondaryIndexType;

	public AbstractSecondaryIndexConfiguration(
			final Class<T> clazz,
			final String attribute,
			final SecondaryIndexType secondaryIndexType ) {
		this(
				clazz,
				Sets.newHashSet(attribute),
				secondaryIndexType);
	}

	public AbstractSecondaryIndexConfiguration(
			final Class<T> clazz,
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType ) {
		super();
		this.clazz = clazz;
		this.attributes = attributes;
		this.secondaryIndexType = secondaryIndexType;
	}

	public Set<String> getAttributes() {
		return attributes;
	}

	@Override
	public void updateType(
			final SimpleFeatureType type ) {
		for (final String attribute : attributes) {
			final AttributeDescriptor desc = type.getDescriptor(attribute);
			if (desc != null) {
				final Class<?> attributeType = desc.getType().getBinding();
				if (clazz.isAssignableFrom(attributeType)) {
					desc.getUserData().put(
							getIndexKey(),
							secondaryIndexType.getValue());
				}
				else {
					LOGGER.error("Expected type " + clazz.getName() + " for attribute '" + attribute + "' but found "
							+ attributeType.getName());
				}
			}
			else {
				LOGGER.error("SimpleFeatureType does not contain an AttributeDescriptor that matches '" + attribute
						+ "'");
			}
		}
	}

	@Override
	public void configureFromType(
			final SimpleFeatureType type ) {
		for (final AttributeDescriptor desc : type.getAttributeDescriptors()) {
			if ((desc.getUserData().get(
					getIndexKey()) != null) && (desc.getUserData().get(
					getIndexKey()).equals(secondaryIndexType.getValue()))) {
				attributes.add(desc.getLocalName());
			}
		}
	}

}
