package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.data.Mutation;

/**
 * This interface is returned by AccumuloOperations and useful for general
 * purpose writing of entries. The default implementation of AccumuloOperations
 * will wrap this interface with a BatchWriter but can be overridden for other
 * mechanisms to write the data.
 */
public interface Writer
{
	public void write(
			Iterable<Mutation> mutations );

	public void write(
			Mutation mutation );

	public void close();

	public void flush();
}
