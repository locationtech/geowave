package mil.nga.giat.geowave.vector.adapter;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * This is used by the FeatureDataAdapter to handle GeoWave 'fields' using
 * SimpleFeature 'attributes.'
 * 
 */
public class FeatureAttributeHandler implements
		NativeFieldHandler<SimpleFeature, Object>
{
	protected final AttributeDescriptor attrDesc;

	public FeatureAttributeHandler(
			final AttributeDescriptor attrDesc ) {
		this.attrDesc = attrDesc;
	}

	@Override
	public ByteArrayId getFieldId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(attrDesc.getLocalName()));
	}

	@Override
	public Object getFieldValue(
			final SimpleFeature row ) {
		return row.getAttribute(attrDesc.getName());
	}
}
