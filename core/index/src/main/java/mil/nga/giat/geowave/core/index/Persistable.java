package mil.nga.giat.geowave.core.index;

/**
 * 
 * A simple interface for persisting objects, PersistenceUtils provides
 * convenience methods for serializing and de-serializing these objects
 * 
 */
public interface Persistable
{
	/**
	 * Convert fields and data within an object to binary form for transmission
	 * or storage.
	 * 
	 * @return an array of bytes representing a binary stream representation of
	 *         the object.
	 */
	public byte[] toBinary();

	/**
	 * Convert a stream of binary bytes to fields and data within an object.
	 * 
	 */
	public void fromBinary(
			byte[] bytes );
}
