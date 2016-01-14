package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class NumericGreaterThanConstraint extends
		NumericQueryConstraint
{

	public NumericGreaterThanConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number,
				Double.MAX_VALUE,
				false,
				true);
	}

}
