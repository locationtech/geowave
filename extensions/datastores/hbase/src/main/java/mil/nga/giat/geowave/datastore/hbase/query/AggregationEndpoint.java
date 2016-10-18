package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.query.generated.AggregationProtos;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class AggregationEndpoint extends
		AggregationProtos.AggregationService implements
		Coprocessor,
		CoprocessorService
{

	private RegionCoprocessorEnvironment env;
	private HBaseDistributableFilter hdFilter;

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
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void aggregate(
			RpcController controller,
			AggregationProtos.AggregationRequest request,
			RpcCallback<AggregationProtos.AggregationResponse> done ) {
		FilterList filterList = null;
		DataAdapter dataAdapter = null;

		AggregationProtos.AggregationResponse response = null;
		ByteString value = ByteString.EMPTY;

		// Get the aggregation type
		String aggregationType = request.getAggregation().getName();
		Aggregation aggregation = null;

		try {
			aggregation = (Aggregation) Class.forName(
					aggregationType).newInstance();

			// Handle aggregation params
			if (request.getAggregation().hasParams()) {
				byte[] parameterBytes = request.getAggregation().getParams().toByteArray();
				final Persistable aggregationParams = PersistenceUtils.fromBinary(
						parameterBytes,
						Persistable.class);
				aggregation.setParameters(aggregationParams);
			}
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		if (aggregation != null) {
			try {
				if (request.hasFilter() && request.hasModel()) {
					hdFilter = new HBaseDistributableFilter();

					byte[] filterBytes = request.getFilter().toByteArray();
					byte[] modelBytes = request.getModel().toByteArray();

					if (hdFilter.init(
							filterBytes,
							modelBytes)) {
						filterList = new FilterList(
								hdFilter);
					}
					else {
						System.err.println("Error creating distributable filter. ");
					}
				}
				else {
					System.err.println("Input distributable filter is undefined. ");
				}
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}

			if (request.hasRangefilter()) {
				byte[] rfilterBytes = request.getRangefilter().toByteArray();

				try {
					MultiRowRangeFilter rangeFilter = MultiRowRangeFilter.parseFrom(rfilterBytes);

					if (filterList == null) {
						filterList = new FilterList(
								rangeFilter);
					}
					else {
						filterList.addFilter(rangeFilter);
					}
				}
				catch (Exception e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}
			else {
				System.err.println("Input range filter is undefined. ");
			}

			if (request.hasAdapter()) {
				byte[] adapterBytes = request.getAdapter().toByteArray();
				dataAdapter = PersistenceUtils.fromBinary(
						adapterBytes,
						DataAdapter.class);
			}

			try {
				Mergeable mvalue = getValue(
						aggregation,
						filterList,
						dataAdapter);

				byte[] bvalue = PersistenceUtils.toBinary(mvalue);
				value = ByteString.copyFrom(bvalue);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
				System.err.println(ioe.getMessage());

				ResponseConverter.setControllerException(
						controller,
						ioe);
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}

		response = AggregationProtos.AggregationResponse.newBuilder().setValue(
				value).build();

		done.run(response);
	}

	private Mergeable getValue(
			Aggregation aggregation,
			Filter filter,
			DataAdapter dataAdapter )
			throws IOException {
		Scan scan = new Scan();
		scan.setMaxVersions(1);

		if (filter != null) {
			scan.setFilter(filter);
		}

		aggregation.clearResult();

		Region region = env.getRegion();

		RegionScanner scanner = region.getScanner(scan);
		region.startRegionOperation();

		List<Cell> results = new ArrayList<Cell>();
		while (scanner.nextRaw(results)) {
			if (dataAdapter != null && hdFilter != null) {
				final Object row = hdFilter.decodeRow(dataAdapter);

				if (row != null) {
					aggregation.aggregate(row);
				}
			}
			else {
				aggregation.aggregate(results);
			}
		}

		scanner.close();
		region.closeRegionOperation();

		return aggregation.getResult();
	}
}
