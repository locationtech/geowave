package mil.nga.giat.geowave.core.store.index.numeric;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class NumericEqualsConstraint extends
		NumericQueryConstraint
{

	public NumericEqualsConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number,
				number,
				true,
				true);
	}
}
