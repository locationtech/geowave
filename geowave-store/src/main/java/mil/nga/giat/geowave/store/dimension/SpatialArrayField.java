package mil.nga.giat.geowave.store.dimension;

public class SpatialArrayField extends
		ArrayField<GeometryWrapper> implements
		DimensionField<ArrayWrapper<GeometryWrapper>>
{

	public SpatialArrayField(
			final DimensionField<GeometryWrapper> elementField ) {
		super(
				elementField);
	}

	public SpatialArrayField() {}

}
