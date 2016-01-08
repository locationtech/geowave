package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.hadoop.conf.Configuration;

public class GeoWaveInputFormatConfiguration implements
		FormatConfiguration
{

	protected boolean isDataWritable = false;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();

	public GeoWaveInputFormatConfiguration() {

	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {
		final GenericStoreCommandLineOptions<DataStore> dataStoreOptions = ((PersistableDataStore) runTimeProperties.getProperty(StoreParam.DATA_STORE)).getCliOptions();
		GeoWaveInputFormat.setDataStoreName(
				configuration,
				dataStoreOptions.getFactory().getName());
		GeoWaveInputFormat.setStoreConfigOptions(
				configuration,
				ConfigUtils.valuesToStrings(
						dataStoreOptions.getConfigOptions(),
						dataStoreOptions.getFactory().getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				configuration,
				dataStoreOptions.getNamespace());

		final String indexId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.INDEX_ID);
		final String adapterId = runTimeProperties.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID);

		if (indexId != null) {
			final PrimaryIndex[] indices = ClusteringUtils.getIndices(runTimeProperties);
			final ByteArrayId byteId = new ByteArrayId(
					StringUtils.stringToBinary(indexId));
			for (final PrimaryIndex index : indices) {
				if (byteId.equals(index.getId())) {
					GeoWaveInputFormat.addIndex(
							configuration,
							index);
				}
			}
		}
		for (final PrimaryIndex index : indices) {
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
		for (final DataAdapter<?> adapter : adapters) {
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
			final PrimaryIndex index ) {
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
			final boolean isWritable ) {
		isDataWritable = isWritable;

	}

	@Override
	public List<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			ExtractParameters.Extract.INDEX_ID,
			ExtractParameters.Extract.ADAPTER_ID,
			ExtractParameters.Extract.QUERY,
			ExtractParameters.Extract.MAX_INPUT_SPLIT,
			ExtractParameters.Extract.MIN_INPUT_SPLIT,
			StoreParam.DATA_STORE
		});
	}
}
