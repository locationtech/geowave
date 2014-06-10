package mil.nga.giat.geowave.store;

import java.io.Closeable;
import java.util.Iterator;

/**
 * This interface wraps both the Iterator interface and the Closeable interface.
 * It is best to close an iterator of this interface when it is no longer
 * needed.
 * 
 * @param <E>
 *            A generic for the type of data for iteration
 */
public interface CloseableIterator<E> extends
		Iterator<E>,
		Closeable
{

}
