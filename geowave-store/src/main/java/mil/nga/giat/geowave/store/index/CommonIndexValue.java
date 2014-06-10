package mil.nga.giat.geowave.store.index;

/**
 * A common index value can be very generic but must have a way to identify its
 * visibility
 * 
 */
public interface CommonIndexValue
{
	public byte[] getVisibility();

	public void setVisibility(
			byte[] visibility );
}
