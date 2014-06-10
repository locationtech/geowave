package mil.nga.giat.geowave.index.dimension;

/**
 * The Latitude Definition class is a convenience class used to define a
 * dimension which is associated with the Y axis on a Cartesian plane.
 * 
 * Minimum bounds = -90 and maximum bounds = 90
 * 
 */
public class LatitudeDefinition extends
		BasicDimensionDefinition
{

	/**
	 * Convenience constructor used to construct a simple latitude dimension
	 * object which sits on a Cartesian plane.
	 * 
	 */
	public LatitudeDefinition() {
		super(
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
			byte[] bytes ) {}
}
