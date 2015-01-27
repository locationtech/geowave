package mil.nga.giat.geowave.index.dimension;

public class HalfResolutionLatitudeDefinition extends
		BasicDimensionDefinition
{

	public HalfResolutionLatitudeDefinition() {
		super(
				-180,
				180);
	}

	@Override
	protected double clamp(
			final double x ) {
		// continue to clamp values between -90 and 90
		return clamp(
				x,
				-90,
				90);
	}

	@Override
	public byte[] toBinary() {
		// essentially all that is needed is the class name for reflection
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}
}
