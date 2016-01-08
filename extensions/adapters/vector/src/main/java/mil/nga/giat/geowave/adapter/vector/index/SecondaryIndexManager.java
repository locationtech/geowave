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
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class SecondaryIndexManager implements
		Persistable
{
	private final List<SecondaryIndex<SimpleFeature>> supportedSecondaryIndices = new ArrayList<>();

	public SecondaryIndexManager(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {
		initialize(
				dataAdapter,
				sft,
				statsManager);
	}

	private void initialize(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {

		final List<ByteArrayId> numericFields = new ArrayList<>();
		final List<ByteArrayId> textFields = new ArrayList<>();
		final List<ByteArrayId> temporalFields = new ArrayList<>();
		final List<DataStatistics<SimpleFeature>> secondaryIndexStatistics = new ArrayList<>();

		for (final AttributeDescriptor desc : sft.getAttributeDescriptors()) {
			final Map<Object, Object> userData = desc.getUserData();
			final String attributeName = desc.getLocalName();
			final ByteArrayId fieldId = new ByteArrayId(
					attributeName);
			if (userData.containsKey(NumericSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					NumericSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				numericFields.add(fieldId);
			}
			else if (userData.containsKey(TextSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					TextSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				textFields.add(fieldId);
			}
			else if (userData.containsKey(TemporalSecondaryIndexConfiguration.INDEX_KEY) && userData.get(
					TemporalSecondaryIndexConfiguration.INDEX_KEY).equals(
					Boolean.TRUE)) {
				temporalFields.add(fieldId);
			}
		}

		if (numericFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> numericStatistics = new ArrayList<>();
			for (final ByteArrayId numericField : numericFields) {
				numericStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						numericField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new NumericIndexStrategy(),
					numericFields.toArray(new ByteArrayId[numericFields.size()]),
					numericStatistics));
			secondaryIndexStatistics.addAll(numericStatistics);
		}
		if (textFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> textStatistics = new ArrayList<>();
			for (final ByteArrayId textField : textFields) {
				textStatistics.add(new FeatureHyperLogLogStatistics(
						dataAdapter.getAdapterId(),
						textField.getString(),
						16));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TextIndexStrategy(),
					textFields.toArray(new ByteArrayId[textFields.size()]),
					textStatistics));
			secondaryIndexStatistics.addAll(textStatistics);
		}
		if (temporalFields.size() > 0) {
			final List<DataStatistics<SimpleFeature>> temporalStatistics = new ArrayList<>();
			for (final ByteArrayId temporalField : temporalFields) {
				temporalStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						temporalField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TemporalIndexStrategy(),
					temporalFields.toArray(new ByteArrayId[temporalFields.size()]),
					temporalStatistics));
			secondaryIndexStatistics.addAll(temporalStatistics);
		}

		for (final DataStatistics<SimpleFeature> secondaryIndexStatistic : secondaryIndexStatistics) {
			statsManager.addStats(
					secondaryIndexStatistic,
					new FieldIdStatisticVisibility<SimpleFeature>(
							secondaryIndexStatistic.getStatisticsId()));
		}
	}

	public List<SecondaryIndex<SimpleFeature>> getSupportedSecondaryIndices() {
		return supportedSecondaryIndices;
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
			byte[] bytes ) {
		final List<Persistable> persistables = PersistenceUtils.fromBinary(bytes);
		for (final Persistable persistable : persistables) {
			supportedSecondaryIndices.add((SecondaryIndex<SimpleFeature>) persistable);
		}
	}

}
