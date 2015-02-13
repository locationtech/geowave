package mil.nga.giat.geowave.accumulo;

import java.util.List;

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
	 * @param additionalAuthorizations
	 *            additional authorizations
	 * @return The appropriate batch deleter
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public BatchDeleter createBatchDeleter(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException;

	/**
	 * Creates a new batch scanner that can be used by an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param additionalAuthorizatios
	 *            additional authorization other than any defaults provided by
	 *            the implementing class
	 * @return The appropriate batch scanner
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public BatchScanner createBatchScanner(
			final String tableName,
			String... additionalAuthorizations )
			throws TableNotFoundException;

	/**
	 * Creates a new scanner that can be used by an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param additionalAuthorizations
	 *            an additional authorization other than any defaults provided
	 *            by the implementing class
	 * @return The appropriate scanner
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Scanner createScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException;

	/**
	 * Creates a table for an index
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 */
	public void createTable(
			final String tableName );

	/**
	 * Creates a new writer that can be used by an index. The basic
	 * implementation uses a BatchWriter but other implementations can be
	 * replaced such as a context-based writer for bulk ingest within a
	 * map-reduce job. A table is created by default if it does not exist with
	 * no custom iterators.
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
	 * map-reduce job. This will use the createTable flag to determine if the
	 * table should be created if it does not exist.
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
	 * Attaches the iterators to the specified table. This will check if the
	 * scope is the same and if the options are the same. If the options are
	 * different, it will use the implementation of
	 * IteratorConfig.mergeOptions() to perform the merge. This will use the
	 * createTable flag to determine if the table should be created if it does
	 * not exist.
	 * 
	 * @param tableName
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param createTable
	 *            If true and the table does not exist, it will be created. If
	 *            false and the table does not exist, a TableNotFoundException
	 *            will be thrown.
	 * @return A flag indicating whether the iterator was successfully attached.
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public boolean attachIterators(
			final String tableName,
			final boolean createTable,
			final IteratorConfig[] iterators )
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
	 * Drops all tables in the instance namespace.  If no tables are found in
	 * the namespace no operation occurs.
	 */
	public void deleteAll()
			throws TableNotFoundException,
			AccumuloException,
			AccumuloSecurityException;

	/**
	 * Drops the specified row from the specified table. Returns whether the
	 * operation completed successfully.
	 * 
	 * @param tableName
	 *            the name of the table to delete from, this must be provided
	 * @param rowId
	 *            the row ID to delete, this must be provided
	 * @param columnFamily
	 *            the column family of the row to delete, this can be null to
	 *            delete the whole row
	 * @param columnQualifier
	 *            the column qualifier for a given column family to delete, this
	 *            can be null to delete the whole column family
	 * @return Returns true if the row deletion didn't encounter any errors,
	 *         false if nothing was found or the row was not dropped
	 *         successfully
	 */
	public boolean delete(
			final String tableName,
			final ByteArrayId rowId,
			String columnFamily,
			String columnQualifier );

	/**
	 * Drops the specified row from the specified table. Returns whether the
	 * operation completed successfully.
	 * 
	 * @param tableName
	 *            the name of the table to delete from, this must be provided
	 * @param rowIds
	 *            the row ID to delete, this must be provided
	 * @param columnFamily
	 *            the column family of the row to delete, this can be null to
	 *            delete the whole row
	 * @param columnQualifier
	 *            the column qualifier for a given column family to delete, this
	 *            can be null to delete the whole column family
	 * @param additionalAuthorizations
	 *            checks authorizations
	 * @return Returns true if the row deletion didn't encounter any errors,
	 *         false if nothing was found or the row was not dropped
	 *         successfully
	 */
	public boolean delete(
			final String tableName,
			final List<ByteArrayId> rowId,
			final String columnFamily,
			final String columnQualifier,
			final String... additionalAuthorizations );

	/**
	 * 
	 * Delete all data associated with a given adapter and index.
	 * 
	 * @param tableName
	 *            the name of the table to delete from, this must be provided
	 * @param columnFamily
	 *            the column family of the row to delete, this can be null to
	 *            delete the whole row
	 * @param additionalAuthorizations
	 *            checks authorizations
	 * @return
	 */
	public boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations );

	/**
	 * 
	 * @param tableName
	 * @param additionalAuthorizations
	 * @return the number of rows in the table given the constraints by the
	 *         provided authorizations
	 */
	public long getRowCount(
			final String tableName,
			String... additionalAuthorizations );

	/**
	 * 
	 * Insure user has the given operations.
	 */
	public void insureAuthorization(
			final String... authorizations )
			throws AccumuloException,
			AccumuloSecurityException;
}
