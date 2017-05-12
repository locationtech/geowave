package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

public abstract class AbstractSecondaryIndexConfiguration<T> implements
		SimpleFeatureSecondaryIndexConfiguration
{

	private static final long serialVersionUID = -7425830022998223202L;
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractSecondaryIndexConfiguration.class);
	private final Class<T> clazz;
	private final Set<String> attributes;
	private final SecondaryIndexType secondaryIndexType;
	private final List<String> fieldIds;

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
		this(
				clazz,
				attributes,
				secondaryIndexType,
				Collections.<String> emptyList());
	}

	public AbstractSecondaryIndexConfiguration(
			final Class<T> clazz,
			final String attribute,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldIds ) {
		this(
				clazz,
				Sets.newHashSet(attribute),
				secondaryIndexType,
				fieldIds);
	}

	public AbstractSecondaryIndexConfiguration(
			final Class<T> clazz,
			final Set<String> attributes,
			final SecondaryIndexType secondaryIndexType,
			final List<String> fieldIds ) {
		super();
		this.clazz = clazz;
		this.attributes = attributes;
		this.secondaryIndexType = secondaryIndexType;
		this.fieldIds = fieldIds;
		if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL) && (fieldIds == null || fieldIds.isEmpty())) {
			throw new RuntimeException(
					"A list of fieldIds must be provided when using a PARTIAL index type");
		}
	}

	public Set<String> getAttributes() {
		return attributes;
	}

	public List<String> getFieldIds() {
		return fieldIds;
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
					if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL)) {
						desc.getUserData().put(
								secondaryIndexType.getValue(),
								Joiner.on(
										",").join(
										fieldIds));
					}
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
			if (desc.getUserData().containsKey(
					SecondaryIndexType.PARTIAL.getValue())) {
				String joined = (String) desc.getUserData().get(
						SecondaryIndexType.PARTIAL.getValue());
				final Iterable<String> fields = Splitter.on(
						",").split(
						joined);
				for (String field : fields) {
					fieldIds.add(field);
				}
			}
		}
	}

}
