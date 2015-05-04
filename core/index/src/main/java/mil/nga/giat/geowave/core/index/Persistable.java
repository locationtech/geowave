package mil.nga.giat.geowave.core.index;

/**
 * 
 * A simple inteface for persisting objects, PersistenceUtils provides
 * convenience methods for serializing and deserializing these objects
 * 
 */
public interface Persistable
{
	public byte[] toBinary();

	public void fromBinary(
			byte[] bytes );
}
