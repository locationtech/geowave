package mil.nga.giat.geowave.adapter.raster.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import org.opengis.coverage.grid.GridCoverage;

public class RasterTileWriter implements
		FieldWriter<GridCoverage, RasterTile<?>>
{
	@Override
	public byte[] getVisibility(
			final GridCoverage rowValue,
			final ByteArrayId fieldId,
			final RasterTile<?> fieldValue ) {
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final RasterTile<?> fieldValue ) {
		// there is no need to preface the payload with the class name and a
		// length of the class name, the implementation is assumed to be known
		// on read so we can save space on persistence
		return fieldValue.toBinary();
	}

}
