package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.AggregationProtos;

public class AggregationEndpoint extends
		AggregationProtos.AggregationService implements
		Coprocessor,
		CoprocessorService
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationEndpoint.class);

	private RegionCoprocessorEnvironment env;

	@Override
	public void start(
			final CoprocessorEnvironment env )
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
			final CoprocessorEnvironment env )
			throws IOException {
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void aggregate(
			final RpcController controller,
			final AggregationProtos.AggregationRequest request,
			final RpcCallback<AggregationProtos.AggregationResponse> done ) {
		FilterList filterList = null;
		final DataAdapter dataAdapter = null;
		ByteArrayId adapterId = null;
		AggregationProtos.AggregationResponse response = null;
		ByteString value = ByteString.EMPTY;

		// Get the aggregation type
		final String aggregationType = request.getAggregation().getName();
		Aggregation aggregation = null;

		try {
			aggregation = (Aggregation) Class.forName(
					aggregationType).newInstance();

			// Handle aggregation params
			if (request.getAggregation().hasParams()) {
				final byte[] parameterBytes = request.getAggregation().getParams().toByteArray();
				final Persistable aggregationParams = PersistenceUtils.fromBinary(
						parameterBytes,
						Persistable.class);
				aggregation.setParameters(aggregationParams);
			}
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			LOGGER.error(
					"Could not create instance of Aggregation Type (" + aggregationType + ")",
					e);
		}
		HBaseDistributableFilter hdFilter = null;
		if (aggregation != null) {

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

			try {
				final Mergeable mvalue = getValue(
						aggregation,
						filterList,
						dataAdapter,
						adapterId,
						hdFilter,
						request.getBlockCaching(),
						request.getCacheSize());

				final byte[] bvalue = PersistenceUtils.toBinary(mvalue);
				value = ByteString.copyFrom(bvalue);
			}
			catch (final IOException ioe) {
				LOGGER.error(
						"Error during aggregation.",
						ioe);

				ResponseConverter.setControllerException(
						controller,
						ioe);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Error during aggregation.",
						e);
			}
		}

		response = AggregationProtos.AggregationResponse.newBuilder().setValue(
				value).build();

		done.run(response);
	}

	private Mergeable getValue(
			final Aggregation aggregation,
			final Filter filter,
			final DataAdapter dataAdapter,
			final ByteArrayId adapterId,
			final HBaseDistributableFilter hdFilter,
			final boolean blockCaching,
			final int scanCacheSize )
			throws IOException {
		final Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setCacheBlocks(blockCaching);

		if (scanCacheSize != HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
			scan.setCaching(scanCacheSize);
		}

		if (filter != null) {
			scan.setFilter(filter);
		}
		if (adapterId != null) {
			scan.addFamily(adapterId.getBytes());
		}

		try (InternalScanner scanner = env.getRegion().getScanner(
				scan)) {
			final List<Cell> results = new ArrayList<Cell>();
			boolean hasNext;
			do {
				hasNext = scanner.next(results);
				if (!results.isEmpty()) {
					if ((dataAdapter != null) && (hdFilter != null)) {
						final Object row = hdFilter.decodeRow(dataAdapter);

						if (row != null) {
							aggregation.aggregate(row);
						}
					}
					else if (hdFilter != null) {
						aggregation.aggregate(hdFilter.getPersistenceEncoding());
					}
					else {
						aggregation.aggregate(null);
					}
					results.clear();
				}
			}
			while (hasNext);
		}
		return aggregation.getResult();
	}
}
