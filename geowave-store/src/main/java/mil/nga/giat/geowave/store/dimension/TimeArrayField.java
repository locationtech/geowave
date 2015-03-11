package mil.nga.giat.geowave.store.dimension;

public class TimeArrayField extends
		ArrayField<Time> implements
		DimensionField<ArrayWrapper<Time>>
{

	public TimeArrayField(
			final DimensionField<Time> elementField ) {
		super(
				elementField);
	}

	public TimeArrayField() {}
}
