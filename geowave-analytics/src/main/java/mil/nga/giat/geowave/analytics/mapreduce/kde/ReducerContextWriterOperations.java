package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.IteratorConfig;
import mil.nga.giat.geowave.accumulo.Writer;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerContextWriterOperations implements
		AccumuloOperations
{
	private final ReducerContextWriter writer;

	public ReducerContextWriterOperations(
			final Context context,
			final String tableName ) {
		writer = new ReducerContextWriter(
				context,
				tableName);
	}
	
	public void insureAuthorization(
			final String... authorizations )
			throws AccumuloException,
			AccumuloSecurityException {
		
	}
	
	@Override
	public Scanner createScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return null;
	}

	@Override
	public Writer createWriter(
			final String tableName )
			throws TableNotFoundException {
		return writer;
	}

	@Override
	public Writer createWriter(
			final String tableName,
			final boolean createTable )
			throws TableNotFoundException {
		return writer;
	}

	@Override
	public boolean deleteTable(
			final String tableName ) {
		return false;
	}

	@Override
	public boolean deleteAll() {
		return false;
	}

	@Override
	public boolean tableExists(
			final String tableName ) {
		return false;
	}

	@Override
	public boolean localityGroupExists(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException {
		return false;
	}

	@Override
	public void addLocalityGroup(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException,
			AccumuloSecurityException {}

	@Override
	public boolean delete(
			final String tableName,
			final ByteArrayId rowId,
			final String columnFamily,
			final String columnQualifier ) {
		return false;
	}

	@Override
	public boolean attachIterators(
			String tableName,
			boolean createTable,
			IteratorConfig[] iterators )
			throws TableNotFoundException {
		return false;
	}

	public void createTable(
			String tableName ) {}

	@Override
	public BatchScanner createBatchScanner(
			String tableName,
			String... additionalAuthorizations )
			throws TableNotFoundException {
		return null;
	}

	@Override
	public BatchDeleter createBatchDeleter(
			String tableName,
			String... additionalAuthorizations )
			throws TableNotFoundException {
		return null;
	}

	@Override
	public boolean delete(
			String tableName,
			List<ByteArrayId> rowId,
			String columnFamily,
			String columnQualifier,
			String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public boolean deleteAll(
			String tableName,
			String columnFamily,
			String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public long getRowCount(
			String tableName,
			String... additionalAuthorizations ) {
		return 0;
	}

	@Override
	public String[] getAuthorizations(
			String... additionalAuthorizations ) {
		return additionalAuthorizations;
	}
}
