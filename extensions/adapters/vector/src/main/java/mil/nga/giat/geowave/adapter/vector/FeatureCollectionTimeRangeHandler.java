package mil.nga.giat.geowave.adapter.vector;

import java.util.Arrays;

import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.TimeRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;

import org.geotools.feature.DefaultFeatureCollection;

/**
 * This class handles the internal responsibility of persisting time ranges
 * based on a start time attribute and end time attribute to and from a GeoWave
 * common index field for SimpleFeature data.
 * 
 */
public class FeatureCollectionTimeRangeHandler implements
		IndexFieldHandler<DefaultFeatureCollection, ArrayWrapper<Time>, Object>
{
	private final FeatureCollectionAttributeHandler nativeStartTimeHandler;
	private final FeatureCollectionAttributeHandler nativeEndTimeHandler;
	private final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler;

	public FeatureCollectionTimeRangeHandler(
			final FeatureCollectionAttributeHandler nativeStartTimeHandler,
			final FeatureCollectionAttributeHandler nativeEndTimeHandler ) {
		this(
				nativeStartTimeHandler,
				nativeEndTimeHandler,
				null);
	}

	public FeatureCollectionTimeRangeHandler(
			final FeatureCollectionAttributeHandler nativeStartTimeHandler,
			final FeatureCollectionAttributeHandler nativeEndTimeHandler,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler ) {
		this.nativeStartTimeHandler = nativeStartTimeHandler;
		this.nativeEndTimeHandler = nativeEndTimeHandler;
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public ByteArrayId[] getNativeFieldIds() {
		return new ByteArrayId[] {
			nativeStartTimeHandler.getFieldId(),
			nativeEndTimeHandler.getFieldId()
		};
	}

	@Override
	public ArrayWrapper<Time> toIndexValue(
			final DefaultFeatureCollection row ) {
		final Object[] startArray = (Object[]) nativeStartTimeHandler.getFieldValue(row);
		final Object[] endArray = (Object[]) nativeEndTimeHandler.getFieldValue(row);

		Arrays.sort(startArray);
		Arrays.sort(endArray);

		byte[] visibility;
		if (visibilityHandler != null) {
			final byte[] startVisibility = visibilityHandler.getVisibility(
					row,
					nativeStartTimeHandler.getFieldId(),
					startArray);
			final byte[] endVisibility = visibilityHandler.getVisibility(
					row,
					nativeEndTimeHandler.getFieldId(),
					endArray);
			if (Arrays.equals(
					startVisibility,
					endVisibility)) {
				// its easy if they both have the same visibility
				visibility = startVisibility;
			}
			else {
				// otherwise the assumption is that we combine the two
				// visibilities
				visibility = ByteArrayUtils.combineArrays(
						startVisibility,
						endVisibility);
			}
		}
		else {
			visibility = new byte[] {};
		}

		final TimeRange[] timeArray = new TimeRange[startArray.length];

		for (int i = 0; i < startArray.length; i++) {
			timeArray[i] = new TimeRange(
					TimeUtils.getTimeMillis(startArray[i]),
					TimeUtils.getTimeMillis(endArray[i]),
					visibility);
		}

		return new ArrayWrapper<Time>(
				timeArray,
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final ArrayWrapper<Time> indexValue ) {

		final Class<?> startBindingClass = nativeStartTimeHandler.attrDesc.getType().getBinding();
		final Class<?> endBindingClass = nativeEndTimeHandler.attrDesc.getType().getBinding();

		final Object[] startTimes = new Object[indexValue.getArray().length];
		final Object[] endTimes = new Object[indexValue.getArray().length];
		for (int i = 0; i < indexValue.getArray().length; i++) {
			final NumericData value = ((TimeRange) indexValue.getArray()[i]).toNumericData();
			startTimes[i] = TimeUtils.getTimeValue(
					startBindingClass,
					(long) value.getMin());
			endTimes[i] = TimeUtils.getTimeValue(
					endBindingClass,
					(long) value.getMax());
		}

		return new PersistentValue[] {
			new PersistentValue<Object>(
					nativeStartTimeHandler.getFieldId(),
					startTimes),
			new PersistentValue<Object>(
					nativeEndTimeHandler.getFieldId(),
					endTimes),
		};
	}
}
