package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

public class CustomCRSSpatialField extends SpatialField {
	private byte axis;
	public CustomCRSSpatialField(){}
	public CustomCRSSpatialField(byte axis, final NumericDimensionDefinition baseDefinition) {
		this(axis,baseDefinition, GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public CustomCRSSpatialField(byte axis,
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId) {
		super(baseDefinition, fieldId);
	}

	@Override
	public NumericData getNumericData(final GeometryWrapper geometry) {
		//TODO if this can be generalized to n-dimensional that would be better
		if (axis == 0){
			return GeometryUtils.xRangeFromGeometry(geometry.getGeometry());
		}
		return GeometryUtils.yRangeFromGeometry(geometry.getGeometry());
	}
	@Override
	public byte[] toBinary() {

		//TODO future issue to investigate performance improvements associated with excessive array/object allocations
		//serialize axis
		byte [] superBinary = super.toBinary();
		byte[] retVal = new byte[superBinary.length + 1];
		System.arraycopy(superBinary, 0, retVal, 0, superBinary.length);
		retVal[superBinary.length] = axis;
		return retVal;
	}
	@Override
	public void fromBinary(byte[] bytes) {
		//TODO future issue to investigate performance improvements associated with excessive array/object allocations
		//deserialize axis
		byte[] superBinary = new byte[bytes.length-1];
		System.arraycopy(bytes, 0, superBinary, 0,superBinary.length );
		super.fromBinary(superBinary);
		axis = bytes[superBinary.length];
	}
}
