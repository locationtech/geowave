package mil.nga.giat.geowave.core.store.entities;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class GeoWaveValueImpl implements
		GeoWaveValue
{
	private final byte[] fieldMask;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveValueImpl(
			final GeoWaveValue[] values ) {
		if ((values == null) || (values.length == 0)) {
			fieldMask = new byte[] {};
			visibility = new byte[] {};
			value = new byte[] {};
		}
		else if (values.length == 1) {
			fieldMask = values[0].getFieldMask();
			visibility = values[0].getVisibility();
			value = values[0].getValue();
		}
		else {
			byte[] intermediateFieldMask = values[0].getFieldMask();
			byte[] intermediateVisibility = values[0].getVisibility();
			byte[] intermediateValue = values[0].getValue();
			for (int i = 1; i < values.length; i++) {
				intermediateFieldMask = BitmaskUtils.generateANDBitmask(
						intermediateFieldMask,
						values[i].getFieldMask());
				intermediateVisibility = DataStoreUtils.mergeVisibilities(
						intermediateVisibility,
						values[i].getVisibility());
				intermediateValue = ByteArrayUtils.combineArrays(
						intermediateValue,
						values[i].getValue());

			}
			fieldMask = intermediateFieldMask;
			visibility = intermediateVisibility;
			value = intermediateValue;
		}
	}

	public GeoWaveValueImpl(
			final byte[] fieldMask,
			final byte[] visibility,
			final byte[] value ) {
		this.fieldMask = fieldMask;
		this.visibility = visibility;
		this.value = value;
	}

	@Override
	public byte[] getFieldMask() {
		return fieldMask;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public byte[] getValue() {
		return value;
	}
}
