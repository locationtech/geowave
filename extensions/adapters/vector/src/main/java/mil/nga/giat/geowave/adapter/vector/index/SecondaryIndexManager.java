package mil.nga.giat.geowave.adapter.vector.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Splitter;

import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.numeric.NumericFieldIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;

public class SecondaryIndexManager implements
		Persistable
{
	private final List<SecondaryIndex<SimpleFeature>> supportedSecondaryIndices = new ArrayList<>();
	private transient DataAdapter<SimpleFeature> dataAdapter;
	private transient SimpleFeatureType sft;
	private transient StatsManager statsManager;

	protected SecondaryIndexManager() {}

	public SecondaryIndexManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {
		this.dataAdapter = dataAdapter;
		this.statsManager = statsManager;
		this.sft = sft;
		initializeIndices();
	}

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

	private void addIndex(
			final String secondaryIndex,
			final ByteArrayId fieldId,
			final SecondaryIndexType secondaryIndexType,
			final List<ByteArrayId> fieldsForPartial ) {
		final List<DataStatistics<SimpleFeature>> statistics = new ArrayList<>();
		DataStatistics<SimpleFeature> stat = null;
		switch (secondaryIndex) {
			case NumericSecondaryIndexConfiguration.INDEX_KEY:
				stat = new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						fieldId.getString());
				statistics.add(stat);
				supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
						new NumericFieldIndexStrategy(),
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
					fieldId);
		}
	}

	@Override
	public byte[] toBinary() {
		final List<Persistable> persistables = new ArrayList<Persistable>();
		for (final SecondaryIndex<SimpleFeature> secondaryIndex : supportedSecondaryIndices) {
			persistables.add(secondaryIndex);
		}
		return PersistenceUtils.toBinary(persistables);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final List<Persistable> persistables = PersistenceUtils.fromBinary(bytes);
		for (final Persistable persistable : persistables) {
			supportedSecondaryIndices.add((SecondaryIndex<SimpleFeature>) persistable);
		}
	}

}