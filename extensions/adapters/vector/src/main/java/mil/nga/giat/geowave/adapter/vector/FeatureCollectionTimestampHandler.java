package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.Timestamp;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;

import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * This class handles the internal responsibility of persisting an array of
 * timestamp instants based on a temporal attribute (a Java binding class of
 * Date or Calendar for an attribute)to and from a GeoWave common index field
 * for SimpleFeature data.
 * 
 */
public class FeatureCollectionTimestampHandler implements
		IndexFieldHandler<DefaultFeatureCollection, ArrayWrapper<Time>, Object>
{
	private final FeatureCollectionAttributeHandler nativeTimestampHandler;
	private final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler;

	public FeatureCollectionTimestampHandler(
			final AttributeDescriptor timestampAttrDesc ) {
		this(
				timestampAttrDesc,
				null);
	}

	public FeatureCollectionTimestampHandler(
			final AttributeDescriptor timestampAttrDesc,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler ) {
		nativeTimestampHandler = new FeatureCollectionAttributeHandler(
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
	public ArrayWrapper<Time> toIndexValue(
			final DefaultFeatureCollection row ) {
		final Object[] objectArray = (Object[]) nativeTimestampHandler.getFieldValue(row);

		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeTimestampHandler.getFieldId(),
					objectArray[0]);
		}
		else {
			visibility = new byte[] {};
		}

		final Timestamp[] times = new Timestamp[objectArray.length];
		for (int i = 0; i < objectArray.length; i++) {
			times[i] = new Timestamp(
					TimeUtils.getTimeMillis(objectArray[i]),
					visibility);
		}

		return new ArrayWrapper<Time>(
				times,
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final ArrayWrapper<Time> indexValue ) {
		final Class<?> bindingClass = nativeTimestampHandler.attrDesc.getType().getBinding();

		final Object[] times = new Object[indexValue.getArray().length];
		for (int i = 0; i < indexValue.getArray().length; i++) {
			times[i] = TimeUtils.getTimeValue(
					bindingClass,
					(long) indexValue.getArray()[i].toNumericData().getCentroid());
		}

		return new PersistentValue[] {
			new PersistentValue<Object>(
					nativeTimestampHandler.getFieldId(),
					times)
		};
	}
}
