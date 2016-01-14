package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class NumericGreaterThanOrEqualToConstraint extends
		NumericQueryConstraint
{

	public NumericGreaterThanOrEqualToConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number,
				Double.MAX_VALUE,
				true,
				true);
	}

}
