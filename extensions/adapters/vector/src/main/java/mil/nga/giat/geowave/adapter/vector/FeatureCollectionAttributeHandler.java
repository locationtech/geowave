package mil.nga.giat.geowave.adapter.vector;

import java.lang.reflect.Array;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * This is used by the FeatureCollectionDataAdapter to handle GeoWave 'fields'
 * using SimpleFeature 'attributes.'
 * 
 */
public class FeatureCollectionAttributeHandler implements
		NativeFieldHandler<DefaultFeatureCollection, Object>
{
	protected final AttributeDescriptor attrDesc;

	public FeatureCollectionAttributeHandler(
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
			final DefaultFeatureCollection row ) {
		final SimpleFeatureIterator itr = row.features();
		final Object[] values = (Object[]) Array.newInstance(
				attrDesc.getType().getBinding(),
				row.size());
		int i = 0;
		while (itr.hasNext()) {
			values[i++] = itr.next().getAttribute(
					attrDesc.getLocalName());
		}
		return values;
	}
}
