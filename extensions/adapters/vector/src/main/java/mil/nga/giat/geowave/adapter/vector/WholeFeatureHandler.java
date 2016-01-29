package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class WholeFeatureHandler implements
		NativeFieldHandler<SimpleFeature, Object>
{
	private final ByteArrayId fieldId;

	public WholeFeatureHandler(
			SimpleFeatureType type ) {
		super();
		fieldId = new ByteArrayId(
				StringUtils.stringToBinary(type.getTypeName()));
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public Object getFieldValue(
			SimpleFeature row ) {
		return row;
	}

}
