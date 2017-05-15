package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import org.apache.hadoop.hbase.filter.FilterList;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDelete.BulkDeleteRequest;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDelete.BulkDeleteResponse;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDelete.BulkDeleteResponse.Builder;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDelete.BulkDeleteService;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDelete.BulkDeleteRequest.BulkDeleteType;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseDistributableFilter;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseNumericIndexStrategyFilter;

import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class HBaseBulkDeleteEndpoint extends
		BulkDeleteService implements
		CoprocessorService,
		Coprocessor
{
	private static final String NO_OF_VERSIONS_TO_DELETE = "noOfVersionsToDelete";
	private final static Logger LOGGER = Logger.getLogger(HBaseBulkDeleteEndpoint.class);

	private RegionCoprocessorEnvironment env;

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void delete(
			RpcController controller,
			BulkDeleteRequest request,
			RpcCallback<BulkDeleteResponse> done ) {
		long totalRowsDeleted = 0L;
		long totalVersionsDeleted = 0L;
		FilterList filterList = null;
		// final DataAdapter dataAdapter = null;
		ByteArrayId adapterId = null;

		Long timestamp = null;
		if (request.hasTimestamp()) {
			timestamp = request.getTimestamp();
		}
		BulkDeleteType deleteType = request.getDeleteType();

		/**
		 * Extract the filter from the bulkDeleteRequest
		 */
		HBaseDistributableFilter hdFilter = null;
		if (request.hasRangeFilter()) {
			final byte[] rfilterBytes = request.getRangeFilter().toByteArray();

			try {
				final MultiRowRangeFilter rangeFilter = MultiRowRangeFilter.parseFrom(rfilterBytes);
				filterList = new FilterList(
						rangeFilter);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Error creating range filter.",
						e);
			}
		}
		else {
			LOGGER.error("Input range filter is undefined.");
		}

		if (request.hasNumericIndexStrategyFilter()) {
			final byte[] nisFilterBytes = request.getNumericIndexStrategyFilter().toByteArray();

			try {
				final HBaseNumericIndexStrategyFilter numericIndexStrategyFilter = HBaseNumericIndexStrategyFilter
						.parseFrom(nisFilterBytes);
				if (filterList == null) {
					filterList = new FilterList(
							numericIndexStrategyFilter);
				}
				else {
					filterList.addFilter(numericIndexStrategyFilter);
				}
			}
			catch (final Exception e) {
				LOGGER.error(
						"Error creating index strategy filter.",
						e);
			}
		}

		try {
			// Add distributable filters if requested, this has to be last
			// in the filter list for the dedupe filter to work correctly
			if (request.hasModel()) {
				hdFilter = new HBaseDistributableFilter();
				final byte[] filterBytes;
				if (request.hasFilter()) {
					filterBytes = request.getFilter().toByteArray();
				}
				else {
					filterBytes = null;
				}
				final byte[] modelBytes = request.getModel().toByteArray();
				if (hdFilter.init(
						filterBytes,
						modelBytes)) {
					if (filterList == null) {
						filterList = new FilterList(
								hdFilter);
					}
					else {
						filterList.addFilter(hdFilter);
					}
				}
				else {
					LOGGER.error("Error creating distributable filter.");
				}
			}
			else {
				LOGGER.error("Input distributable filter is undefined.");
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error creating distributable filter.",
					e);
		}

		if (request.hasAdapter()) {
			final byte[] adapterBytes = request.getAdapter().toByteArray();
			final ByteBuffer buf = ByteBuffer.wrap(adapterBytes);
			buf.get();
			final int length = buf.getInt();
			final byte[] adapterIdBytes = new byte[length];
			buf.get(adapterIdBytes);
			adapterId = new ByteArrayId(
					adapterIdBytes);
		}
		else if (request.hasAdapterId()) {
			final byte[] adapterIdBytes = request.getAdapterId().toByteArray();
			adapterId = new ByteArrayId(
					adapterIdBytes);
		}

		/**
		 * Start the actual delete process
		 */
		RegionScanner scanner = null;
		try {
			scanner = null;
			Scan scan = new Scan();
			scan.setFilter(filterList);

			if (adapterId != null) {
				scan.addFamily(adapterId.getBytes());
			}

			Region region = env.getRegion();
			scanner = region.getScanner(scan);

			boolean hasMore = true;
			int rowBatchSize = request.getRowBatchSize();
			while (hasMore) {
				List<List<Cell>> deleteRows = new ArrayList<>(
						rowBatchSize);
				for (int i = 0; i < rowBatchSize; i++) {
					List<Cell> results = new ArrayList<>();
					hasMore = scanner.next(results);
					if (results.size() > 0) {
						deleteRows.add(results);
					}
					if (!hasMore) {
						// There are no more rows.
						break;
					}
				}
				if (deleteRows.size() > 0) {
					Mutation[] deleteArr = new Mutation[deleteRows.size()];
					int i = 0;
					for (List<Cell> deleteRow : deleteRows) {
						deleteArr[i++] = createDeleteMutation(
								deleteRow,
								deleteType,
								timestamp);
					}
					OperationStatus[] opStatus = region.batchMutate(
							deleteArr,
							HConstants.NO_NONCE,
							HConstants.NO_NONCE);
					for (i = 0; i < opStatus.length; i++) {
						if (opStatus[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
							break;
						}
						totalRowsDeleted++;
						if (deleteType == BulkDeleteType.VERSION) {
							byte[] versionsDeleted = deleteArr[i].getAttribute(NO_OF_VERSIONS_TO_DELETE);
							if (versionsDeleted != null) {
								totalVersionsDeleted += Bytes.toInt(versionsDeleted);
							}
						}
					}
				}
			}
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			if (scanner != null) {
				try {
					scanner.close();
				}
				catch (IOException ioe) {
					LOGGER.error(
							"Error during bulk delete in HBase.",
							ioe);
					;
				}
			}
		}

		Builder responseBuilder = BulkDeleteResponse.newBuilder();
		responseBuilder.setRowsDeleted(totalRowsDeleted);
		if (deleteType == BulkDeleteType.VERSION) {
			responseBuilder.setVersionsDeleted(totalVersionsDeleted);
		}

		// Send the response back
		BulkDeleteResponse response = responseBuilder.build();
		done.run(response);
	}

	private Delete createDeleteMutation(
			List<Cell> deleteRow,
			BulkDeleteType deleteType,
			Long timestamp ) {
		long ts;
		if (timestamp == null) {
			ts = HConstants.LATEST_TIMESTAMP;
		}
		else {
			ts = timestamp;
		}
		// We just need the rowkey. Get it from 1st KV.
		byte[] row = CellUtil.cloneRow(deleteRow.get(0));
		Delete delete = new Delete(
				row,
				ts);
		if (deleteType == BulkDeleteType.FAMILY) {
			Set<byte[]> families = new TreeSet<>(
					Bytes.BYTES_COMPARATOR);
			for (Cell kv : deleteRow) {
				if (families.add(CellUtil.cloneFamily(kv))) {
					delete.addFamily(
							CellUtil.cloneFamily(kv),
							ts);
				}
			}
		}
		else if (deleteType == BulkDeleteType.COLUMN) {
			Set<Column> columns = new HashSet<>();
			for (Cell kv : deleteRow) {
				Column column = new Column(
						CellUtil.cloneFamily(kv),
						CellUtil.cloneQualifier(kv));
				if (columns.add(column)) {
					// Making deleteColumns() calls more than once for the same
					// cf:qualifier is not correct
					// Every call to deleteColumns() will add a new KV to the
					// familymap which will finally
					// get written to the memstore as part of delete().
					delete.addColumns(
							column.family,
							column.qualifier,
							ts);
				}
			}
		}
		else if (deleteType == BulkDeleteType.VERSION) {
			// When some timestamp was passed to the delete() call only one
			// version of the column (with
			// given timestamp) will be deleted. If no timestamp passed, it will
			// delete N versions.
			// How many versions will get deleted depends on the Scan being
			// passed. All the KVs that
			// the scan fetched will get deleted.
			int noOfVersionsToDelete = 0;
			if (timestamp == null) {
				for (Cell kv : deleteRow) {
					delete.addColumn(
							CellUtil.cloneFamily(kv),
							CellUtil.cloneQualifier(kv),
							kv.getTimestamp());
					noOfVersionsToDelete++;
				}
			}
			else {
				Set<Column> columns = new HashSet<>();
				for (Cell kv : deleteRow) {
					Column column = new Column(
							CellUtil.cloneFamily(kv),
							CellUtil.cloneQualifier(kv));
					// Only one version of particular column getting deleted.
					if (columns.add(column)) {
						delete.addColumn(
								column.family,
								column.qualifier,
								ts);
						noOfVersionsToDelete++;
					}
				}
			}
			delete.setAttribute(
					NO_OF_VERSIONS_TO_DELETE,
					Bytes.toBytes(noOfVersionsToDelete));
		}
		return delete;
	}

	private static class Column
	{
		private byte[] family;
		private byte[] qualifier;

		public Column(
				byte[] family,
				byte[] qualifier ) {
			this.family = family;
			this.qualifier = qualifier;
		}

		@Override
		public boolean equals(
				Object other ) {
			if (!(other instanceof Column)) {
				return false;
			}
			Column column = (Column) other;
			return Bytes.equals(
					this.family,
					column.family) && Bytes.equals(
					this.qualifier,
					column.qualifier);
		}

		@Override
		public int hashCode() {
			int h = 31;
			h = h + 13 * Bytes.hashCode(this.family);
			h = h + 13 * Bytes.hashCode(this.qualifier);
			return h;
		}
	}

	@Override
	public void start(
			CoprocessorEnvironment env )
			throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		}
		else {
			throw new CoprocessorException(
					"Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(
			CoprocessorEnvironment env )
			throws IOException {
		// nothing to do
	}
}