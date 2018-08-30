package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;

public class CustomCRSBoundedSpatialDimension extends
		BasicDimensionDefinition implements
		CustomCRSSpatialDimension
{
	private BaseCustomCRSSpatialDimension baseCustomCRS;

	public CustomCRSBoundedSpatialDimension() {}

	public CustomCRSBoundedSpatialDimension(
			byte axis,
			final double min,
			final double max ) {
		super(
				min,
				max);
		baseCustomCRS = new BaseCustomCRSSpatialDimension(
				axis);

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((baseCustomCRS == null) ? 0 : baseCustomCRS.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (getClass() != obj.getClass()) return false;
		CustomCRSBoundedSpatialDimension other = (CustomCRSBoundedSpatialDimension) obj;
		if (baseCustomCRS == null) {
			if (other.baseCustomCRS != null) return false;
		}
		else if (!baseCustomCRS.equals(other.baseCustomCRS)) return false;
		return true;
	}

	@Override
	public byte[] toBinary() {

		// TODO future issue to investigate performance improvements associated
		// with excessive array/object allocations
		// serialize axis
		return baseCustomCRS.addAxisToBinary(super.toBinary());
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		// TODO future issue to investigate performance improvements associated
		// with excessive array/object allocations
		// deserialize axis
		baseCustomCRS = new BaseCustomCRSSpatialDimension();
		super.fromBinary(baseCustomCRS.getAxisFromBinaryAndRemove(bytes));
	}

	public byte getAxis() {
		return baseCustomCRS.getAxis();
	}
}
