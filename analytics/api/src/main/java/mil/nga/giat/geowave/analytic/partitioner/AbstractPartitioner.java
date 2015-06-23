package mil.nga.giat.geowave.analytic.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;
import mil.nga.giat.geowave.analytic.model.SpatialIndexModelBuilder;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * Basic support class for Partitioners (e.g {@link Paritioner}
 * 
 * @param <T>
 */
public abstract class AbstractPartitioner<T> implements
		Partitioner<T>
{

	private transient Index index = null;
	private transient double[] distancePerDimension = null;
	private transient double precisionFactor = 1.0;

	public AbstractPartitioner() {
		distancePerDimension = new double[0];
	}

	public AbstractPartitioner(
			final CommonIndexModel indexModel,
			final double[] distancePerDimension ) {
		super();
		this.distancePerDimension = distancePerDimension;
		this.initIndex(
				indexModel,
				distancePerDimension);

	}

	public AbstractPartitioner(
			final double[] distancePerDimension ) {
		super();
		this.distancePerDimension = distancePerDimension;
	}

	protected double[] getDistancePerDimension() {
		return distancePerDimension;
	}

	protected Index getIndex() {
		return index;
	}

	@Override
	public List<PartitionData> getCubeIdentifiers(
			final T entry ) {
		final Set<PartitionData> partitionIdSet = new HashSet<PartitionData>();

		final NumericDataHolder numericData = getNumericData(entry);
		if (numericData == null) {
			return Collections.emptyList();
		}
		addPartitions(
				partitionIdSet,
				getIndex().getIndexStrategy().getInsertionIds(
						numericData.primary),
				true);

		for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
			addPartitions(
					partitionIdSet,
					getIndex().getIndexStrategy().getInsertionIds(
							expansionData),
					false);
		}
		return new ArrayList<PartitionData>(
				partitionIdSet);
	}

	@Override
	public void partition(
			final T entry,
			final PartitionDataCallback callback )
			throws Exception {
		final NumericDataHolder numericData = getNumericData(entry);
		if (numericData == null) {
			return;
		}
		for (final ByteArrayId addId : getIndex().getIndexStrategy().getInsertionIds(
				numericData.primary)) {
			callback.partitionWith(new PartitionData(
					addId,
					true));
		}

		for (final MultiDimensionalNumericData expansionData : numericData.expansion) {
			for (final ByteArrayId addId : getIndex().getIndexStrategy().getInsertionIds(
					expansionData)) {
				callback.partitionWith(new PartitionData(
						addId,
						false));
			}
		}
	}

	protected static class NumericDataHolder
	{
		MultiDimensionalNumericData primary;
		MultiDimensionalNumericData[] expansion;
	}

	protected abstract NumericDataHolder getNumericData(
			final T entry );

	public MultiDimensionalNumericData getRangesForPartition(
			final PartitionData partitionData ) {
		return index.getIndexStrategy().getRangeForId(
				partitionData.getId());
	}

	protected void addPartitions(
			final Set<PartitionData> masterList,
			final List<ByteArrayId> addList,
			final boolean isPrimary ) {
		for (final ByteArrayId addId : addList) {
			masterList.add(new PartitionData(
					addId,
					isPrimary));
		}
	}

	public static void putDistances(
			final PropertyManagement config,
			final double[] distances ) {

		final StringBuffer buffer = new StringBuffer();
		for (final double distance : distances) {
			buffer.append(distance);
			buffer.append(',');
		}
		config.storeIfEmpty(
				ClusteringParameters.Clustering.DISTANCE_THRESHOLDS,
				buffer.substring(
						0,
						buffer.length() - 1));

		config.store(
				ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT,
				"m");

	}

	public static double[] getDistances(
			final ConfigurationWrapper config,
			final Class<?> classContext ) {
		final String distances = config.getString(
				ClusteringParameters.Clustering.DISTANCE_THRESHOLDS,
				classContext,
				"0.000001");

		final String distancesArray[] = distances.split(",");
		final double[] distancePerDimension = new double[distancesArray.length];
		{
			int i = 0;
			for (final String eachDistance : distancesArray) {
				distancePerDimension[i++] = Double.valueOf(eachDistance);
			}
		}
		return distancePerDimension;
	}

	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {

		distancePerDimension = getDistances(
				context,
				this.getClass());

		this.precisionFactor = context.getDouble(
				Partition.PARTITION_PRECISION,
				this.getClass(),
				1.0);

		if ((precisionFactor < 0) || (precisionFactor > 1.0)) {
			throw new IllegalArgumentException(
					String.format(
							"Precision value must be between 0 and 1: %.6f",
							precisionFactor));
		}

		try {
			final IndexModelBuilder builder = context.getInstance(
					CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
					this.getClass(),
					IndexModelBuilder.class,
					SpatialIndexModelBuilder.class);

			final CommonIndexModel model = builder.buildModel();
			if (model.getDimensions().length > distancePerDimension.length) {
				final double[] newDistancePerDimension = new double[model.getDimensions().length];
				for (int j = 0; j < newDistancePerDimension.length; j++) {
					newDistancePerDimension[j] = distancePerDimension[j < distancePerDimension.length ? j : (distancePerDimension.length - 1)];
				}
				distancePerDimension = newDistancePerDimension;
			}
			this.initIndex(
					model,
					distancePerDimension);

		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(
					e);
		}

	}

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration ) {
		RunnerUtils.setParameter(
				configuration,
				getClass(),
				runTimeProperties,
				new ParameterEnum[] {
					CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
					ClusteringParameters.Clustering.DISTANCE_THRESHOLDS,
					PartitionParameters.Partition.PARTITION_PRECISION
				});
	}

	protected void initIndex(
			final CommonIndexModel indexModel,
			final double[] distancePerDimensionForIndex ) {

		// truncating to lower precision
		final DimensionField<?>[] dimensions = indexModel.getDimensions();

		int totalRequestedPrecision = 0;
		final int[] dimensionPrecision = new int[indexModel.getDimensions().length];
		for (int i = 0; i < dimensionPrecision.length; i++) {
			final double distance = distancePerDimensionForIndex[i] * 2.0; // total
			// width...(radius)
			// adjust by precision factory (0 to 1.0)
			dimensionPrecision[i] = (int) (precisionFactor * Math.abs((int) (Math.log(dimensions[i].getRange() / distance) / Math.log(2))));

			totalRequestedPrecision += dimensionPrecision[i];
		}
		if (totalRequestedPrecision > 63) {
			final double rescale = 63.0 / totalRequestedPrecision;
			for (int i = 0; i < dimensionPrecision.length; i++) {
				dimensionPrecision[i] = (int) (rescale * dimensionPrecision[i]);
			}
		}

		final TieredSFCIndexStrategy indexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
				indexModel.getDimensions(),
				dimensionPrecision,
				SFCType.HILBERT);

		// Not relevant since this is a single tier strategy.
		// For now, just setting to a non-zero reasonable value
		indexStrategy.setMaxEstimatedDuplicateIds((int) Math.pow(
				dimensions.length,
				4));

		index = new Index(
				indexStrategy,
				indexModel);

	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS
				});
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.DISTANCE_THRESHOLDS
				});

	}

}
