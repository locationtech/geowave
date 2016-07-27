package mil.nga.giat.geowave.adapter.vector.index;

import java.util.ArrayList;
import java.util.HashMap;
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

	private void addFieldToMap(
			final Map<SecondaryIndexType, List<ByteArrayId>> fieldMap,
			final SecondaryIndexType secondaryIndexType,
			final ByteArrayId fieldId ) {
		if (fieldMap.containsKey(secondaryIndexType)) {
			final List<ByteArrayId> fields = fieldMap.get(secondaryIndexType);
			fields.add(fieldId);
		}
		else {
			final List<ByteArrayId> fields = new ArrayList<>();
			fields.add(fieldId);
			fieldMap.put(
					secondaryIndexType,
					fields);
		}
	}

	private void initialize(
			final DataAdapter<SimpleFeature> dataAdapter,
			final SimpleFeatureType sft,
			final StatsManager statsManager ) {

		final Map<SecondaryIndexType, List<ByteArrayId>> numericFields = new HashMap<>();
		final Map<SecondaryIndexType, List<ByteArrayId>> textFields = new HashMap<>();
		final Map<SecondaryIndexType, List<ByteArrayId>> temporalFields = new HashMap<>();
		final List<DataStatistics<SimpleFeature>> secondaryIndexStatistics = new ArrayList<>();

		for (final AttributeDescriptor desc : sft.getAttributeDescriptors()) {
			final Map<Object, Object> userData = desc.getUserData();
			final String attributeName = desc.getLocalName();
			final ByteArrayId fieldId = new ByteArrayId(
					attributeName);
			SecondaryIndexType secondaryIndexType = null;
			if (userData.containsKey(NumericSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(NumericSecondaryIndexConfiguration.INDEX_KEY));
				addFieldToMap(
						numericFields,
						secondaryIndexType,
						fieldId);
			}
			else if (userData.containsKey(TextSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TextSecondaryIndexConfiguration.INDEX_KEY));
				addFieldToMap(
						textFields,
						secondaryIndexType,
						fieldId);
			}
			else if (userData.containsKey(TemporalSecondaryIndexConfiguration.INDEX_KEY)) {
				secondaryIndexType = SecondaryIndexType.valueOf((String) userData
						.get(TemporalSecondaryIndexConfiguration.INDEX_KEY));
				addFieldToMap(
						temporalFields,
						secondaryIndexType,
						fieldId);
			}
		}

		for (final Map.Entry<SecondaryIndexType, List<ByteArrayId>> entry : numericFields.entrySet()) {
			final SecondaryIndexType secondaryIndexType = entry.getKey();
			final List<ByteArrayId> fieldIds = entry.getValue();
			final List<DataStatistics<SimpleFeature>> numericStatistics = new ArrayList<>();
			for (final ByteArrayId numericField : fieldIds) {
				numericStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						numericField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new NumericIndexStrategy(),
					fieldIds.toArray(new ByteArrayId[fieldIds.size()]),
					numericStatistics,
					secondaryIndexType));
			secondaryIndexStatistics.addAll(numericStatistics);
		}
		for (final Map.Entry<SecondaryIndexType, List<ByteArrayId>> entry : textFields.entrySet()) {
			final SecondaryIndexType secondaryIndexType = entry.getKey();
			final List<ByteArrayId> fieldIds = entry.getValue();
			final List<DataStatistics<SimpleFeature>> textStatistics = new ArrayList<>();
			for (final ByteArrayId textField : fieldIds) {
				textStatistics.add(new FeatureHyperLogLogStatistics(
						dataAdapter.getAdapterId(),
						textField.getString(),
						16));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TextIndexStrategy(),
					fieldIds.toArray(new ByteArrayId[fieldIds.size()]),
					textStatistics,
					secondaryIndexType));
			secondaryIndexStatistics.addAll(textStatistics);
		}
		for (final Map.Entry<SecondaryIndexType, List<ByteArrayId>> entry : temporalFields.entrySet()) {
			final SecondaryIndexType secondaryIndexType = entry.getKey();
			final List<ByteArrayId> fieldIds = entry.getValue();
			final List<DataStatistics<SimpleFeature>> temporalStatistics = new ArrayList<>();
			for (final ByteArrayId temporalField : fieldIds) {
				temporalStatistics.add(new FeatureNumericHistogramStatistics(
						dataAdapter.getAdapterId(),
						temporalField.getString()));
			}
			supportedSecondaryIndices.add(new SecondaryIndex<SimpleFeature>(
					new TemporalIndexStrategy(),
					fieldIds.toArray(new ByteArrayId[fieldIds.size()]),
					temporalStatistics,
					secondaryIndexType));
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