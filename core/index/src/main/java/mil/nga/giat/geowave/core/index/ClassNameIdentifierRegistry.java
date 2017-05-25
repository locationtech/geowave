/**
 * 
 */
package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class registry for mapping GeoWave class names to a specific numeric
 * identifier for use during serialization and deserialization. During
 * serialization, a class name is attempted to be mapped to a registered
 * identifier. During deserialization, the identifier is mapped to the
 * associated class name. If the class is not registered, it will be converted
 * to the default binary representation and not converted to an associated
 * numeric identifier.
 */
public class ClassNameIdentifierRegistry
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ClassNameIdentifierRegistry.class);

	private static Map<Short, String> classNames;
	private static Map<String, Short> classNameIdentifiers;

	private static void registerClassIdentifier(
			final String className,
			final Short identifier )
			throws Exception {
		if (getClassNameIdentifiers().containsKey(
				className)) {
			throw new Exception(
					"Class [" + className + "] already registered");
		}
		if (getClassNames().containsKey(
				identifier)) {
			throw new Exception(
					"Identifier [" + identifier + "] already registered");
		}
		getClassNameIdentifiers().put(
				className,
				identifier);

		getClassNames().put(
				identifier,
				className);
	}

	public static Map<Short, String> getClassNames() {
		if (classNames == null) {
			classNames = Collections.synchronizedMap(new HashMap<Short, String>());
		}
		return classNames;
	}

	public static Map<String, Short> getClassNameIdentifiers() {
		if (classNameIdentifiers == null) {
			classNameIdentifiers = Collections.synchronizedMap(new HashMap<String, Short>());
		}
		return classNameIdentifiers;
	}

	static {
		try {
			/*
			 * Register legacy (mil.nga.giat) class names
			 */
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.CompoundIndexStrategy$CompoundIndexMetaDataWrapper",
					(short) 101);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy$TierIndexMetaData",
					(short) 102);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.CompoundIndexStrategy",
					(short) 103);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy",
					(short) 104);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.hilbert.HilbertSFC",
					(short) 105);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition",
					(short) 106);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition",
					(short) 107);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy",
					(short) 108);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.simple.HashKeyIndexStrategy",
					(short) 109);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$MockAbstractDataAdapter",
					(short) 110);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics",
					(short) 111);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$TestPersistentIndexFieldHandler",
					(short) 112);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.MockComponents$IntegerRangeDataStatistics",
					(short) 113);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics",
					(short) 114);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.temporal.DateRangeFilter",
					(short) 115);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.DedupeFilter",
					(short) 116);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.BasicQueryFilter",
					(short) 117);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics",
					(short) 118);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.IndexMetaDataSet",
					(short) 119);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount",
					(short) 120);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount",
					(short) 121);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager",
					(short) 122);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics",
					(short) 123);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics",
					(short) 124);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics",
					(short) 125);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics",
					(short) 126);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.store.PersistableStore",
					(short) 127);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter",
					(short) 128);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy$XZHierarchicalIndexMetaData",
					(short) 129);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.PrimaryIndex",
					(short) 130);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField",
					(short) 131);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition",
					(short) 132);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField",
					(short) 133);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition",
					(short) 134);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.BasicIndexModel",
					(short) 135);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery",
					(short) 136);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.AdapterToIndexMapping",
					(short) 137);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.CustomIdIndex",
					(short) 138);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy",
					(short) 139);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$GeoBoundingBoxStatistics",
					(short) 140);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter",
					(short) 141);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.filter.DistributableFilterList",
					(short) 142);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset",
					(short) 143);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.data.NumericRange",
					(short) 144);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy",
					(short) 145);
			registerClassIdentifier(
					"mil.nga.giat.geowave.format.gpx.GpxIngestPlugin$IngestGpxTrackFromHdfs",
					(short) 146);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition",
					(short) 147);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.query.QueryOptions",
					(short) 148);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter",
					(short) 149);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.merge.RootMergeStrategy",
					(short) 150);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy",
					(short) 151);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataByFilter",
					(short) 152);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.stats.OverviewStatistics",
					(short) 153);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.Resolution",
					(short) 154);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.stats.RasterBoundingBoxStatistics",
					(short) 155);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.stats.HistogramConfig",
					(short) 156);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.geotime.store.dimension.TimeField",
					(short) 157);
			registerClassIdentifier(
					"mil.nga.giat.geowave.format.avro.AvroIngestPlugin$IngestAvroFeaturesFromHdfs",
					(short) 158);

			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataBySampleIndex",
					(short) 159);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileRowTransform",
					(short) 160);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.raster.stats.HistogramStatistics",
					(short) 161);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.query.cql.CQLQueryFilter",
					(short) 162);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics",
					(short) 163);
			registerClassIdentifier(
					"mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics",
					(short) 164);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy",
					(short) 165);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy",
					(short) 166);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.SecondaryIndex",
					(short) 167);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy",
					(short) 168);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy",
					(short) 169);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.query.aggregate.CountResult",
					(short) 170);

			/*
			 * Registering legacy (mil.nga.giat) test class names for objects
			 * that get generated
			 */
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
					(short) 1001);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.PersistenceUtilsTest$APersistable",
					(short) 1002);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
					(short) 1003);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
					(short) 1004);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
					(short) 1005);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
					(short) 1006);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
					(short) 1007);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
					(short) 1008);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
					(short) 1009);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
					(short) 1010);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
					(short) 1011);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$SummingMergeStrategy",
					(short) 1012);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$SumAndAveragingMergeStrategy",
					(short) 1013);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$MergeCounter",
					(short) 1014);

			/*
			 * Register future (org.locationtech) class names
			 */
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.CompoundIndexStrategy$CompoundIndexMetaDataWrapper",
					(short) 501);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy$TierIndexMetaData",
					(short) 502);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.CompoundIndexStrategy",
					(short) 503);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy",
					(short) 504);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.hilbert.HilbertSFC",
					(short) 505);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition",
					(short) 506);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition",
					(short) 507);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.simple.RoundRobinKeyIndexStrategy",
					(short) 508);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.simple.HashKeyIndexStrategy",
					(short) 509);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$MockAbstractDataAdapter",
					(short) 510);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics",
					(short) 511);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$TestPersistentIndexFieldHandler",
					(short) 512);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$IntegerRangeDataStatistics",
					(short) 513);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.RowRangeDataStatistics",
					(short) 514);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.temporal.DateRangeFilter",
					(short) 515);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.DedupeFilter",
					(short) 516);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.BasicQueryFilter",
					(short) 517);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics",
					(short) 518);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.IndexMetaDataSet",
					(short) 519);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount",
					(short) 520);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount",
					(short) 521);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.index.SecondaryIndexManager",
					(short) 522);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics",
					(short) 523);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics",
					(short) 524);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureTimeRangeStatistics",
					(short) 525);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics",
					(short) 526);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.store.PersistableStore",
					(short) 527);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.FeatureDataAdapter",
					(short) 528);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy$XZHierarchicalIndexMetaData",
					(short) 529);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.PrimaryIndex",
					(short) 530);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.LongitudeField",
					(short) 531);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition",
					(short) 532);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.LatitudeField",
					(short) 533);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition",
					(short) 534);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.BasicIndexModel",
					(short) 535);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.query.SpatialQuery",
					(short) 536);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.AdapterToIndexMapping",
					(short) 537);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.CustomIdIndex",
					(short) 538);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy",
					(short) 539);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$GeoBoundingBoxStatistics",
					(short) 540);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.filter.SpatialQueryFilter",
					(short) 541);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.DistributableFilterList",
					(short) 542);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset",
					(short) 543);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.data.NumericRange",
					(short) 544);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy",
					(short) 545);
			registerClassIdentifier(
					"org.locationtech.geowave.format.gpx.GpxIngestPlugin$IngestGpxTrackFromHdfs",
					(short) 546);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition",
					(short) 547);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.query.QueryOptions",
					(short) 548);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter",
					(short) 549);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.RootMergeStrategy",
					(short) 550);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy",
					(short) 551);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataByFilter",
					(short) 552);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.OverviewStatistics",
					(short) 553);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.Resolution",
					(short) 554);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.RasterBoundingBoxStatistics",
					(short) 555);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.HistogramConfig",
					(short) 556);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.TimeField",
					(short) 557);
			registerClassIdentifier(
					"org.locationtech.geowave.format.avro.AvroIngestPlugin$IngestAvroFeaturesFromHdfs",
					(short) 558);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataBySampleIndex",
					(short) 559);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileRowTransform",
					(short) 560);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.HistogramStatistics",
					(short) 561);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.query.cql.CQLQueryFilter",
					(short) 562);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics",
					(short) 563);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics",
					(short) 564);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.tiered.SingleTierSubStrategy",
					(short) 565);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.numeric.NumericIndexStrategy",
					(short) 566);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.SecondaryIndex",
					(short) 567);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.temporal.TemporalIndexStrategy",
					(short) 568);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.text.TextIndexStrategy",
					(short) 569);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.query.aggregate.CountResult",
					(short) 570);
			/*
			 * Registering future (org.locationtech) test class names for
			 * objects that get generated
			 */
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
					(short) 1501);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.PersistenceUtilsTest$APersistable",
					(short) 1502);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
					(short) 1503);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
					(short) 1504);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
					(short) 1505);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
					(short) 1506);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
					(short) 1507);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
					(short) 1508);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
					(short) 1509);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
					(short) 1510);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
					(short) 1511);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$SummingMergeStrategy",
					(short) 1512);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$SumAndAveragingMergeStrategy",
					(short) 1513);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$MergeCounter",
					(short) 1514);
		}
		catch (Exception ex) {
			LOGGER.error(
					"Error registering class identifier: " + ex.getLocalizedMessage(),
					ex);
		}
	}
}
