package mil.nga.giat.geowave.raster.adapter;

import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.data.field.FieldReader;

public class RasterTileReader implements
		FieldReader<RasterTile<?>>
{

	@Override
	public RasterTile<?> readField(
			final byte[] fieldData ) {

		// the class name is not prefaced in the payload, we are assuming it is
		// a raster tile implementation and instantiating it directly

		final RasterTile retVal = PersistenceUtils.classFactory(
				RasterTile.class.getName(),
				RasterTile.class);
		if (retVal != null) {
			retVal.fromBinary(fieldData);
		}
		return retVal;
	}

}
