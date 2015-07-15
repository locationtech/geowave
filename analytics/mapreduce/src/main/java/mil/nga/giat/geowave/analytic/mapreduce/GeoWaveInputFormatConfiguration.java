package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

public class GeoWaveInputFormatConfiguration implements
		FormatConfiguration
{

	protected boolean isDataWritable = false;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<Index> indices = new ArrayList<Index>();

	public GeoWaveInputFormatConfiguration() {

	}

	@Override
	public void setup(
			PropertyManagement runTimeProperties,
			Configuration configuration )
			throws Exception {
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				configuration,
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ZOOKEEKER,
						"localhost:2181"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_INSTANCE,
						"miniInstance"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_USER,
						"root"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_PASSWORD,
						"password"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_NAMESPACE,
						"undefined"));

		final String indexId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.INDEX_ID);
		final String adapterId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID);

		if (indexId != null) {
			final Index[] indices = ClusteringUtils.getIndices(runTimeProperties);
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(indexId));
			for (final Index index : indices) {
				if (byteId.equals(index.getId())) {
					GeoWaveInputFormat.addIndex(
							configuration,
							index);
				}
			}
		}
		for (Index index : indices) {
			GeoWaveInputFormat.addIndex(
					configuration,
					index);
		}

		if (adapterId != null) {
			final DataAdapter<?>[] adapters = ClusteringUtils.getAdapters(runTimeProperties);
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(adapterId));
			for (final DataAdapter<?> adapter : adapters) {
				if (byteId.equals(adapter.getAdapterId())) {
					GeoWaveInputFormat.addDataAdapter(
							configuration,
							adapter);
				}
			}
		}
		for (DataAdapter<?> adapter : adapters) {
			GeoWaveInputFormat.addDataAdapter(
					configuration,
					adapter);
		}

		final DistributableQuery query = runTimeProperties.getPropertyAsQuery(ExtractParameters.Extract.QUERY);

		if (query != null) {
			GeoWaveInputFormat.setQuery(
					configuration,
					query);
		}

		final QueryOptions queryoptions = runTimeProperties.getPropertyAsQueryOptions(ExtractParameters.Extract.QUERY_OPTIONS);

		if (queryoptions != null) {
			GeoWaveInputFormat.setQueryOptions(
					configuration,
					queryoptions);
		}

		final int minInputSplits = runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MIN_INPUT_SPLIT,
				-1);
		if (minInputSplits > 0) {
			GeoWaveInputFormat.setMinimumSplitCount(
					configuration,
					minInputSplits);
		}
		final int maxInputSplits = runTimeProperties.getPropertyAsInt(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				-1);
		if (maxInputSplits > 0) {
			GeoWaveInputFormat.setMaximumSplitCount(
					configuration,
					maxInputSplits);
		}

		GeoWaveInputFormat.setIsOutputWritable(
				configuration,
				isDataWritable);
	}

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final Index index ) {
		indices.add(index);
	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveInputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return isDataWritable;
	}

	@Override
	public void setDataIsWritable(
			boolean isWritable ) {
		isDataWritable = isWritable;

	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE
				});

		ExtractParameters.fillOptions(
				options,
				new ExtractParameters.Extract[] {
					ExtractParameters.Extract.INDEX_ID,
					ExtractParameters.Extract.ADAPTER_ID,
					ExtractParameters.Extract.QUERY,
					ExtractParameters.Extract.QUERY_OPTIONS,
					ExtractParameters.Extract.MAX_INPUT_SPLIT,
					ExtractParameters.Extract.MIN_INPUT_SPLIT
				});

	}
}
