package mil.nga.giat.geowave.adapter.vector;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.Timestamp;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * This class handles the internal responsibility of persisting single timestamp
 * instants based on a temporal attribute (a Java binding class of Date or
 * Calendar for an attribute)to and from a GeoWave common index field for
 * SimpleFeature data.
 * 
 */
public class FeatureTimestampHandler implements
		IndexFieldHandler<SimpleFeature, Time, Object>
{
	private final FeatureAttributeHandler nativeTimestampHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;

	public FeatureTimestampHandler(
			AttributeDescriptor timestampAttrDesc ) {
		this(
				timestampAttrDesc,
				null);
	}

	public FeatureTimestampHandler(
			AttributeDescriptor timestampAttrDesc,
			FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		this.nativeTimestampHandler = new FeatureAttributeHandler(
				timestampAttrDesc);
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public ByteArrayId[] getNativeFieldIds() {
		return new ByteArrayId[] {
			nativeTimestampHandler.getFieldId()
		};
	}

	@Override
	public Time toIndexValue(
			SimpleFeature row ) {
		Object object = nativeTimestampHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeTimestampHandler.getFieldId(),
					object);
		}
		else {
			visibility = new byte[] {};
		}
		return new Timestamp(
				TimeUtils.getTimeMillis(object),
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			Time indexValue ) {
		Class<?> bindingClass = nativeTimestampHandler.attrDesc.getType().getBinding();
		Object obj = TimeUtils.getTimeValue(
				bindingClass,
				(long) indexValue.toNumericData().getCentroid());
		return new PersistentValue[] {
			new PersistentValue<Object>(
					nativeTimestampHandler.getFieldId(),
					obj)
		};
	}
}
