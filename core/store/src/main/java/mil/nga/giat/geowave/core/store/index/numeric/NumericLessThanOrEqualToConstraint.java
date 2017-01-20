package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class NumericLessThanOrEqualToConstraint extends
		NumericQueryConstraint
{

	public NumericLessThanOrEqualToConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				Double.MIN_VALUE,
				number,
				true,
				true);
	}

}
