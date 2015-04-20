package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.dimension.ArrayAdapter;
import mil.nga.giat.geowave.core.store.dimension.ArrayField;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

public class TimeArrayField extends
		ArrayField<Time> implements
		DimensionField<ArrayWrapper<Time>>
{
	private ArrayAdapter<Time> adapter;

	public TimeArrayField(
			final DimensionField<Time> elementField ) {
		super(
				elementField);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	public TimeArrayField() {}

	@Override
	public FieldWriter<?, ArrayWrapper<Time>> getWriter() {
		return adapter;
	}

	@Override
	public FieldReader<ArrayWrapper<Time>> getReader() {
		return adapter;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		super.fromBinary(bytes);
		adapter = new ArrayAdapter<Time>(
				new FixedSizeObjectArrayReader(
						elementField.getReader()),
				new FixedSizeObjectArrayWriter(
						elementField.getWriter()));
	}

	@Override
	public boolean isCompatibleDefinition(
			NumericDimensionDefinition otherDimensionDefinition ) {
		return equals(otherDimensionDefinition);
	}
}
