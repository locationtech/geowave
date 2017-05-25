/**
 * 
 */
package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class ClassNameIdentifierRegistry
{

	public static Map<Integer, String> classNames;
	public static Map<String, Integer> classNameIdentifiers;

	static {
		if (classNameIdentifiers == null) {
			classNameIdentifiers = Collections.synchronizedMap(new HashMap<String, Integer>());
		}
		if (classNames == null) {
			classNames = Collections.synchronizedMap(new HashMap<Integer, String>());
		}

		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.CompoundIndexStrategy$CompoundIndexMetaDataWrapper",
				1294679476);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy$TierIndexMetaData",
				288866390);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.CompoundIndexStrategy",
				1471453559);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy",
				499726105);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.hilbert.HilbertSFC",
				212629858);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition",
				1298940070);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition",
				107912906);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy",
				1796628339);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy",
				1110877739);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.MockComponents$MockAbstractDataAdapter",
				1374439089);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics",
				1522073051);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.MockComponents$TestPersistentIndexFieldHandler",
				2068611194);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.MockComponents$IntegerRangeDataStatistics",
				1522649501);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics",
				1911924659);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.index.temporal.DateRangeFilter",
				1005134672);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.filter.DedupeFilter",
				983529852);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.filter.BasicQueryFilter",
				952477460);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics",
				1378392039);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.index.IndexMetaDataSet",
				1540986920);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount",
				113106634);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount",
				964635613);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager",
				1120108140);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics",
				323388389);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics",
				392370534);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics",
				1521909207);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics",
				303414487);
		registerClassIdentifier(
				"mil.nga.giat.geowave.analytic.store.PersistableStore",
				608933167);
		registerClassIdentifier(
				"mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter",
				795689109);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy$XZHierarchicalIndexMetaData",
				2014911244);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.index.PrimaryIndex",
				1012068953);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField",
				1005789573);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition",
				745774536);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField",
				1970655874);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition",
				928493032);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.index.BasicIndexModel",
				1612959976);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery",
				1644498222);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.AdapterToIndexMapping",
				901419915);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.index.CustomIdIndex",
				1940654064);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy",
				618932487);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$GeoBoundingBoxStatistics",
				717968536);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter",
				403948715);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.filter.DistributableFilterList",
				280374904);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset",
				1180378558);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.sfc.data.NumericRange",
				1746414124);

		// Tests
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
				211151481);
		registerClassIdentifier(
				"mil.nga.giat.geowave.core.index.PersistenceUtilsTest$APersistable",
				956075605);
		registerClassIdentifier(
				"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
				15915066);
		registerClassIdentifier(
				"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
				910737790);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
				1188436383);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
				1229552866);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
				1009951410);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
				870963194);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
				845610688);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
				405493696);
		registerClassIdentifier(
				"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
				374078023);
	}

	private static void registerClassIdentifier(
			final String className,
			final Integer identifier ) {
		classNameIdentifiers.put(
				className,
				identifier);
		classNames.put(
				identifier,
				className);
	}

	public static Integer getClassNameHash(
			final String className ) {
		return ThreadLocalRandom.current().nextInt(
				Integer.MAX_VALUE - 1);
	}
}
