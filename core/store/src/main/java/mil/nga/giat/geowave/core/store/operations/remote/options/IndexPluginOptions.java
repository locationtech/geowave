package mil.nga.giat.geowave.core.store.operations.remote.options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultPluginOptions;
import mil.nga.giat.geowave.core.cli.api.PluginOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeRegistry;

/**
 * This class is responsible for loading index SPI plugins and populating
 * parameters delegate with relevant options for that index.
 */
public class IndexPluginOptions extends
		DefaultPluginOptions implements
		PluginOptions
{

	public static final String INDEX_PROPERTY_NAMESPACE = "index";
	public static final String DEFAULT_PROPERTY_NAMESPACE = "indexdefault";

	private final static Logger LOGGER = LoggerFactory.getLogger(IndexPluginOptions.class);

	private String indexType;

	@Parameter(names = {
		"--indexName"
	}, description = "A custom name can be given to this index. Default name will be the based on configuration parameters.")
	protected String nameOverride = null;

	@Parameter(names = {
		"-np",
		"--numPartitions"
	}, description = "The number of partitions.  Default partitions will be 1.")
	protected int numPartitions = 1;

	@Parameter(names = {
		"-ps",
		"--partitionStrategy"
	}, description = "The partition strategy to use.  Default will be none.")
	protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

	// This is the plugin loaded from SPI based on "type"
	private DimensionalityTypeProviderSpi indexPlugin = null;

	// These are the options loaded from indexPlugin based on "type"
	@ParametersDelegate
	private DimensionalityTypeOptions indexOptions = null;

	/**
	 * Constructor
	 */
	public IndexPluginOptions() {

	}

	@Override
	public void selectPlugin(
			String qualifier ) {
		// Load the Index options.
		indexType = qualifier;
		if (qualifier != null) {
			indexPlugin = DimensionalityTypeRegistry.getSelectedDimensionalityProvider(qualifier);
			if (indexPlugin == null) {
				throw new ParameterException(
						"Unknown index type specified");
			}
			indexOptions = indexPlugin.getOptions();
		}
		else {
			indexPlugin = null;
			indexOptions = null;
		}
	}

	@Override
	public String getType() {
		return indexType;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public String getNameOverride() {
		return nameOverride;
	}

	public PartitionStrategy getPartitionStrategy() {
		return partitionStrategy;
	}

	public DimensionalityTypeProviderSpi getIndexPlugin() {
		return indexPlugin;
	}

	public PrimaryIndex createPrimaryIndex() {
		PrimaryIndex index = indexPlugin.createPrimaryIndex();
		return wrapIndexWithOptions(index);
	}

	private PrimaryIndex wrapIndexWithOptions(
			final PrimaryIndex index ) {
		PrimaryIndex retVal = index;
		if ((numPartitions > 1) && partitionStrategy.equals(PartitionStrategy.ROUND_ROBIN)) {
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new RoundRobinKeyIndexStrategy(
									numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.ROUND_ROBIN.name() + "_"
									+ numPartitions));
		}
		else if (numPartitions > 1) {
			// default to round robin partitioning (none is not valid if there
			// are more than 1 partition)
			if (partitionStrategy.equals(PartitionStrategy.NONE)) {
				LOGGER
						.warn("Partition strategy is necessary when using more than 1 partition, defaulting to 'hash' partitioning.");
			}
			retVal = new CustomIdIndex(
					new CompoundIndexStrategy(
							new HashKeyIndexStrategy(
									numPartitions),
							index.getIndexStrategy()),
					index.getIndexModel(),
					new ByteArrayId(
							index.getId().getString() + "_" + PartitionStrategy.HASH.name() + "_" + numPartitions));
		}
		if ((getNameOverride() != null) && (getNameOverride().length() > 0)) {
			retVal = new CustomIdIndex(
					retVal.getIndexStrategy(),
					retVal.getIndexModel(),
					new ByteArrayId(
							getNameOverride()));
		}
		return retVal;
	}

	public static String getIndexNamespace(
			String name ) {
		return String.format(
				"%s.%s",
				INDEX_PROPERTY_NAMESPACE,
				name);
	}

	public static enum PartitionStrategy {
		NONE,
		HASH,
		ROUND_ROBIN;
	}
}