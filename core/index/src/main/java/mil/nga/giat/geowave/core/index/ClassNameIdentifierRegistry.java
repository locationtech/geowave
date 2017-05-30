/**
 * 
 */
package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ClassNameIdentifierRegistry
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ClassNameIdentifierRegistry.class);
	
	public static Map<Short, String> classNames;
	public static Map<String, Short> classNameIdentifiers;

	static {
		if (classNameIdentifiers == null) {
			classNameIdentifiers = Collections.synchronizedMap(new HashMap<String, Short>());
		}
		if (classNames == null) {
			classNames = Collections.synchronizedMap(new HashMap<Short, String>());
		}

		try {
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.CompoundIndexStrategy$CompoundIndexMetaDataWrapper",
					(short) 15016);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy$TierIndexMetaData",
					(short) 23217);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.CompoundIndexStrategy",
					(short) 28428);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy",
					(short) 19317);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.hilbert.HilbertSFC",
					(short) 4782);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition",
					(short) 8264);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition",
					(short) 17135);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy",
					(short) 2012);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy",
					(short) 26073);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$MockAbstractDataAdapter",
					(short) 28767);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics",
					(short) 6627);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$TestPersistentIndexFieldHandler",
					(short) 1539);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$IntegerRangeDataStatistics",
					(short) 10945);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics",
					(short) 11241);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.temporal.DateRangeFilter",
					(short) 14928);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.DedupeFilter",
					(short) 24578);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.BasicQueryFilter",
					(short) 12932);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics",
					(short) 17261);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.IndexMetaDataSet",
					(short) 28672);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount",
					(short) 8782);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount",
					(short) 30577);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager",
					(short) 9142);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics",
					(short) 27363);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics",
					(short) 26038);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics",
					(short) 2425);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics",
					(short) 13306);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.store.PersistableStore",
					(short) 11024);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter",
					(short) 27935);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy$XZHierarchicalIndexMetaData",
					(short) 9427);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.PrimaryIndex",
					(short) 30511);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField",
					(short) 31107);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition",
					(short) 28785);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField",
					(short) 414);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition",
					(short) 28718);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.BasicIndexModel",
					(short) 18862);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery",
					(short) 6688);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.AdapterToIndexMapping",
					(short) 31615);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.CustomIdIndex",
					(short) 16290);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy",
					(short) 32367);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$GeoBoundingBoxStatistics",
					(short) 14770);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter",
					(short) 32053);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.DistributableFilterList",
					(short) 7259);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset",
					(short) 2953);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.data.NumericRange",
					(short) 24113);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
					(short) 20777);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.PersistenceUtilsTest$APersistable",
					(short) 29248);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
					(short) 19378);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
					(short) 21876);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
					(short) 26520);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
					(short) 17503);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
					(short) 10825);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
					(short) 23933);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
					(short) 23081);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
					(short) 18553);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
					(short) 12152);
		}
		catch (Exception ex) {
			LOGGER.error("Error registering class identifier: "+ex.getLocalizedMessage(), ex);
		}
	}

	private static void registerClassIdentifier(
			final String className,
			final Short identifier )
			throws Exception {
		if (classNameIdentifiers.containsKey(className)) {
			throw new Exception(
					"Class [" + className + " already registered");
		}
		if (classNames.containsKey(identifier)) {
			throw new Exception(
					"Identifier [" + identifier + " already registered");
		}
		classNameIdentifiers.put(
				className,
				identifier);
		classNames.put(
				identifier,
				className);
	}

	public static short getNewClassNameIdentifier() {
		return (short) ThreadLocalRandom.current().nextInt(
				Short.MAX_VALUE + 1);
	}
}
