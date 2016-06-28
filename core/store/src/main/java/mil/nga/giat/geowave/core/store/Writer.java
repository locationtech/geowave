package mil.nga.giat.geowave.core.store;

/**
 * This interface is returned by DataStoreOperations and useful for general
 * purpose writing of entries. The default implementation of AccumuloOperations
 * will wrap this interface with a BatchWriter but can be overridden for other
 * mechanisms to write the data.
 */
public interface Writer<MutationType> extends
		Closable
{
	public void write(
			Iterable<MutationType> mutations );

	public void write(
			MutationType mutation );

	public void flush();
}
