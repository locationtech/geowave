package mil.nga.giat.geowave.adapter.vector.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.FieldIdStatisticVisibility;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Splitter;

/**
 * Class to manage secondary indexes for a Simple Feature Type. It keeps a list
 * of supported secondary indices associated with all the attributes attached to
 * the SimpleFeatureType provided upon instantiation.
 */

public class SecondaryIndexManager implements
		Persistable
{
	private final List<SecondaryIndex<SimpleFeature>> supportedSecondaryIndices = new ArrayList<>();
	private transient DataAdapter<SimpleFeature> dataAdapter;
	private transient SimpleFeatureType sft;
	private transient StatsManager statsManager;

	@Deprecated
	protected SecondaryIndexManager() {}

	/**
	 * Create a SecondaryIndexManager for the given DataAdapter and
	 * SimpleFeatureType, while providing
	 * 
	 * @param dataAdapter
	 * @param sft
	 * @param statsManager
	 */
	public SecondaryIndexManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {
		this.dataAdapter = dataAdapter;
		this.statsManager = statsManager;
		this.sft = sft;
		initializeIndices();
	}

	/**
	 * For every attribute of the SFT to be managed by this index, determine
	 * type and if found, create a secondaryIndex of strategy type Temporal,
	 * Text or Numeric for that attribute.
	 */
	private void initializeIndices() {
		for (final AttributeDescriptor desc : sft.getAttributeDescriptors()) {

			final Map<Object, Object> userData = desc.getUserData();
			final String attributeName = desc.getLocalName();
			final ByteArrayId fieldId = new ByteArrayId(
					attributeName);
			String secondaryIndex = null;
			SecondaryIndexType secondaryIndexType = null;
			final List<ByteArrayId> fieldsForPartial = new ArrayList<>();

			if (userData.containsKey(NumericSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = NumericSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(NumericSecondaryIndexConfiguration.INDEX_KEY));
			}
			else if (userData.containsKey(TextSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = TextSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TextSecondaryIndexConfiguration.INDEX_KEY));
			}
			else if (userData.containsKey(TemporalSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndex = TemporalSecondaryIndexConfiguration.INDEX_KEY;
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TemporalSecondaryIndexConfiguration.INDEX_KEY));
			}

			// If a valid secondary index type is provided, and the type is
			// PARTIAL, then
			// go through list of fields to be joined at add to tracked list of
			// fieldsForPartial

			if (secondaryIndexType != null) {
				if (secondaryIndexType.equals(SecondaryIndexType.PARTIAL)) {
					final String joined = (String) userData.get(SecondaryIndexType.PARTIAL.getValue());
					final Iterable<String> split = Splitter.on(
							",").split(
							joined);
					for (final String field : split) {
						fieldsForPartial.add(new ByteArrayId(
								field));
					}
				}
				addIndex(
						secondaryIndex,
						fieldId,
						secondaryIndexType,
						fieldsForPartial);
			}
		}
	}

	public List<SecondaryIndex<SimpleFeature>> getSupportedSecondaryIndices() {
		return supportedSecondaryIndices;
	}

	/**
	 * Add an index-based secondary index key
	 * 
	 * @param secondaryIndexKey
	 * @param fieldId
	 * @param secondaryIndexType
	 * @param fieldsForPartial
	 */

	private void addIndex(
			final String secondaryIndexKey,
			final ByteArrayId fieldId,
			final SecondaryIndexType secondaryIndexType,
			final List<ByteArrayId> fieldsForPartial ) {

		final List<DataStatistics<SimpleFeature>> statistics = new ArrayList<>();
		DataStatistics<SimpleFeature> stat = null;
		switch (secondaryIndexKey) {

			case NumericSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						fieldId.getString());
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
						new NumericIndexStrategy(),
						fieldId,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			case TextSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureHyperLogLogStatistics(
						dataAdapter.getAdapterId(),
						fieldId.getString(),
						16);
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
						new TextIndexStrategy(),
						fieldId,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			case TemporalSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						fieldId.getString());
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
						new TemporalIndexStrategy(),
						fieldId,
						statistics,
						secondaryIndexType,
						fieldsForPartial));
				break;

			default:
				break;

		}
		for (final DataStatistics<SimpleFeature> statistic : statistics) {
			statsManager.addStats(
					statistic,
					new FieldIdStatisticVisibility<SimpleFeature>(
							statistic.getStatisticsId()));
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * This consists of converting supported secondary indices.
	 */
	@Override
	public byte[] toBinary() {
		final List<Persistable> persistables = new ArrayList<Persistable>();
		for (final SecondaryIndex<SimpleFeature> secondaryIndex : supportedSecondaryIndices) {
			persistables.add(secondaryIndex);
		}
		return PersistenceUtils.toBinary(persistables);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * This extracts the supported secondary indices from the binary stream and
	 * adds them in this object.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			byte[] bytes ) {
		final List<Persistable> persistables = PersistenceUtils.fromBinary(bytes);
		for (final Persistable persistable : persistables) {
			supportedSecondaryIndices.add((SecondaryIndex<SimpleFeature>) persistable);
		}
	}

	/**
	 *
	 * @return the StatsManager object being used by this SecondaryIndex.
	 */
	public StatsManager getStatsManager() {
		return statsManager;
	}

}