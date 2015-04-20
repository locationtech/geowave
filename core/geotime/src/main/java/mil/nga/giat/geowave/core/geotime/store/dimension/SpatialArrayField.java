package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.dimension.ArrayAdapter;
import mil.nga.giat.geowave.core.store.dimension.ArrayField;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

public class SpatialArrayField extends
		ArrayField<GeometryWrapper> implements
		DimensionField<ArrayWrapper<GeometryWrapper>>
{
	private ArrayAdapter<GeometryWrapper> adapter;

	public SpatialArrayField(
			final DimensionField<GeometryWrapper> elementField ) {
		super(
				elementField);
		adapter = new ArrayAdapter<GeometryWrapper>(
				new VariableSizeObjectArrayReader(
						elementField.getReader()),
				new VariableSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public SpatialArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<GeometryWrapper>> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<ArrayWrapper<GeometryWrapper>> getReader() {
		return adapter;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<GeometryWrapper>(
				new VariableSizeObjectArrayReader(
						elementField.getReader()),
				new VariableSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	@Override
	public boolean isCompatibleDefinition(
			NumericDimensionDefinition otherDimensionDefinition ) {
		return equals(otherDimensionDefinition);
	}
}
