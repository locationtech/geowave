package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.ArrayAdapter;
import mil.nga.giat.geowave.core.store.dimension.ArrayField;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

public class SpatialArrayField extends
		ArrayField<GeometryWrapper> implements
		NumericDimensionField<ArrayWrapper<GeometryWrapper>>
{
	private ArrayAdapter<GeometryWrapper> adapter;

	public SpatialArrayField(
			final NumericDimensionField<GeometryWrapper> elementField ) {
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
			final byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<GeometryWrapper>(
				new VariableSizeObjectArrayReader(
						elementField.getReader()),
				new VariableSizeObjectArrayWriter(
						elementField.getWriter()));
	}
}
