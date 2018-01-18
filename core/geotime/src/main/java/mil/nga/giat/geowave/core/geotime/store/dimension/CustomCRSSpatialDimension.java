package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;

public class CustomCRSSpatialDimension extends
		BasicDimensionDefinition
{
	protected byte axis;

	public CustomCRSSpatialDimension() {}

	public CustomCRSSpatialDimension(
			byte axis,
			final double min,
			final double max ) {
		super(
				min,
				max);
		this.axis = axis;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + axis;
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		CustomCRSSpatialDimension other = (CustomCRSSpatialDimension) obj;
		if (axis != other.axis) return false;
		return true;
	}

	@Override
	public byte[] toBinary() {

		// TODO future issue to investigate performance improvements associated
		// with excessive array/object allocations
		// serialize axis
		byte[] superBinary = super.toBinary();
		byte[] retVal = new byte[superBinary.length + 1];
		System.arraycopy(
				superBinary,
				0,
				retVal,
				0,
				superBinary.length);
		retVal[superBinary.length] = axis;
		return retVal;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		// TODO future issue to investigate performance improvements associated
		// with excessive array/object allocations
		// deserialize axis
		byte[] superBinary = new byte[bytes.length - 1];
		System.arraycopy(
				bytes,
				0,
				superBinary,
				0,
				superBinary.length);
		super.fromBinary(superBinary);
		axis = bytes[superBinary.length];
	}
}
