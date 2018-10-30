package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

public class FieldNameParam implements
		Persistable
{
	// TODO we can also include a requested CRS in case we want to reproject
	// (although it seemingly can just as easily be done on the resulting
	// envelope rather than per feature)
	private String fieldName;

	public FieldNameParam() {
		this(
				null);
	}

	public FieldNameParam(
			final String fieldName ) {
		this.fieldName = fieldName;
	}

	@Override
	public byte[] toBinary() {
		if ((fieldName == null) || fieldName.isEmpty()) {
			return new byte[0];
		}
		return StringUtils.stringToBinary(fieldName);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			fieldName = StringUtils.stringFromBinary(bytes);
		}
		else {
			fieldName = null;
		}
	}

	protected boolean isEmpty() {
		return (fieldName == null) || fieldName.isEmpty();
	}

	public String getFieldName() {
		return fieldName;
	}
}
