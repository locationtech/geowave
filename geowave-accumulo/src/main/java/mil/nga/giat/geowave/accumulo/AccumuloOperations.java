package mil.nga.giat.geowave.accumulo;

import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

/**
 * This interface is used as a basis for establishing connections for queries
 * and ingest processes used by the index classes.
 */
public interface AccumuloOperations
{

	/**
	 * Creates a new batch deleter that can be used by an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return The appropriate batch deleter
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public BatchDeleter createBatchDeleter(
			final String tableName )
			throws TableNotFoundException;

	/**
	 * Creates a new batch scanner that can be used by an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return The appropriate batch scanner
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public BatchScanner createBatchScanner(
			final String tableName )
			throws TableNotFoundException;

	/**
	 * Creates a new scanner that can be used by an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return The appropriate scanner
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Scanner createScanner(
			final String tableName )
			throws TableNotFoundException;

	/**
	 * Creates a new writer that can be used by an index. The basic
	 * implementation uses a BatchWriter but other implementations can be
	 * replaced such as a context-based writer for bulk ingest within a
	 * map-reduce job.
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return The appropriate writer
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Writer createWriter(
			final String tableName )
			throws TableNotFoundException;

	/**
	 * Creates a new writer that can be used by an index. The basic
	 * implementation uses a BatchWriter but other implementations can be
	 * replaced such as a context-based writer for bulk ingest within a
	 * map-reduce job.
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param createTable
	 *            If true and the table does not exist, it will be created. If
	 *            false and the table does not exist, a TableNotFoundException
	 *            will be thrown.
	 * @return The appropriate writer
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Writer createWriter(
			final String tableName,
			final boolean createTable )
			throws TableNotFoundException;

	/**
	 * Drops the table with the given name (the basic implementation will use a
	 * table namespace prefix if given). Returns whether the table was found and
	 * the operation completed successfully.
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return Returns true if the table was found and dropped, false if it was
	 *         not found or not dropped successfully
	 */
	public boolean deleteTable(
			final String tableName );

	/**
	 * Checks for the existence of the table with the given name
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @return Returns true if the table was found, false if it was not found
	 */
	public boolean tableExists(
			final String tableName );

	/**
	 * Checks for the existence of the locality group with the given name,
	 * within the table of the given name
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param localityGroup
	 *            The name of the locality group
	 * @return Returns true if the locality group was found, false if it was not
	 *         found
	 */
	public boolean localityGroupExists(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException;

	/**
	 * Adds the locality group with the given name to the table of the given
	 * name
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param localityGroup
	 *            The name of the locality group
	 * @throws AccumuloSecurityException
	 */
	public void addLocalityGroup(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException,
			AccumuloSecurityException;

	/**
	 * Drops all tables in the given namespace. Returns whether any tables were
	 * found and the operation completed successfully.
	 * 
	 * @return Returns true if at least one table was found and dropped with the
	 *         given namespace, false if nothing was found or it was not dropped
	 *         successfully
	 */
	public boolean deleteAll();

	/**
	 * Drops the specified row from the specified table. Returns whether the
	 * operation completed successfully.
	 * 
	 * @return Returns true if the row deletion didn't encounter any errors,
	 *         false if nothing was found or the row was not dropped
	 *         successfully
	 */
	public boolean deleteRow(
			final String tableName,
			final ByteArrayId rowId );
}
