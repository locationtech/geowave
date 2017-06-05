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

	private static void registerClassIdentifier(
			final String className,
			final Short identifier )
			throws Exception {
		if (classNameIdentifiers.containsKey(className)) {
			throw new Exception(
					"Class [" + className + "] already registered");
		}
		if (classNames.containsKey(identifier)) {
			throw new Exception(
					"Identifier [" + identifier + "] already registered");
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

	static {
		if (classNameIdentifiers == null) {
			classNameIdentifiers = Collections.synchronizedMap(new HashMap<String, Short>());
		}
		if (classNames == null) {
			classNames = Collections.synchronizedMap(new HashMap<Short, String>());
		}

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

			/*
			 * Registering legacy (mil.nga.giat) test class names for objects
			 * that get generated
			 * 
			 * keep numbering for tests in the 200's
			 */
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
					(short) 201);
			registerClassIdentifier(
					"mil.nga.giat.geowave.core.index.PersistenceUtilsTest$APersistable",
					(short) 202);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
					(short) 203);
			registerClassIdentifier(
					"mil.nga.giat.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
					(short) 204);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
					(short) 205);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
					(short) 206);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
					(short) 207);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
					(short) 208);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
					(short) 209);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
					(short) 210);
			registerClassIdentifier(
					"mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
					(short) 211);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$SummingMergeStrategy",
					(short) 212);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$SumAndAveragingMergeStrategy",
					(short) 213);
			registerClassIdentifier(
					"mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT$MergeCounter",
					(short) 214);

			/*
			 * Register future (org.locationtech) class names
			 */
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.CompoundIndexStrategy$CompoundIndexMetaDataWrapper",
					(short) 301);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy$TierIndexMetaData",
					(short) 302);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.CompoundIndexStrategy",
					(short) 303);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy",
					(short) 304);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.hilbert.HilbertSFC",
					(short) 305);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition",
					(short) 306);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition",
					(short) 307);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.simple.RoundRobinKeyIndexStrategy",
					(short) 308);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.simple.HashKeyIndexStrategy",
					(short) 309);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$MockAbstractDataAdapter",
					(short) 310);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics",
					(short) 311);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$TestPersistentIndexFieldHandler",
					(short) 312);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.MockComponents$IntegerRangeDataStatistics",
					(short) 313);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.RowRangeDataStatistics",
					(short) 314);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.temporal.DateRangeFilter",
					(short) 315);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.DedupeFilter",
					(short) 316);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.BasicQueryFilter",
					(short) 317);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics",
					(short) 318);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.IndexMetaDataSet",
					(short) 319);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount",
					(short) 320);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount",
					(short) 321);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.index.SecondaryIndexManager",
					(short) 322);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics",
					(short) 323);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics",
					(short) 324);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureTimeRangeStatistics",
					(short) 325);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics",
					(short) 326);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.store.PersistableStore",
					(short) 327);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.vector.FeatureDataAdapter",
					(short) 328);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy$XZHierarchicalIndexMetaData",
					(short) 329);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.PrimaryIndex",
					(short) 330);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.LongitudeField",
					(short) 331);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition",
					(short) 332);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.LatitudeField",
					(short) 333);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition",
					(short) 334);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.BasicIndexModel",
					(short) 335);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.query.SpatialQuery",
					(short) 336);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.AdapterToIndexMapping",
					(short) 337);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.index.CustomIdIndex",
					(short) 338);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy",
					(short) 339);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$GeoBoundingBoxStatistics",
					(short) 340);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.filter.SpatialQueryFilter",
					(short) 341);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.filter.DistributableFilterList",
					(short) 342);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset",
					(short) 343);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.sfc.data.NumericRange",
					(short) 344);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy",
					(short) 345);
			registerClassIdentifier(
					"org.locationtech.geowave.format.gpx.GpxIngestPlugin$IngestGpxTrackFromHdfs",
					(short) 346);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition",
					(short) 347);
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.query.QueryOptions",
					(short) 348);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter",
					(short) 349);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.RootMergeStrategy",
					(short) 350);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy",
					(short) 351);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataByFilter",
					(short) 352);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.OverviewStatistics",
					(short) 353);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.Resolution",
					(short) 354);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.RasterBoundingBoxStatistics",
					(short) 355);
			registerClassIdentifier(
					"org.locationtech.geowave.adapter.raster.stats.HistogramConfig",
					(short) 356);
			registerClassIdentifier(
					"org.locationtech.geowave.core.geotime.store.dimension.TimeField",
					(short) 357);
			registerClassIdentifier(
					"org.locationtech.geowave.format.avro.AvroIngestPlugin$IngestAvroFeaturesFromHdfs",
					(short) 358);

			/*
			 * Registering future (org.locationtech) test class names for
			 * objects that get generated
			 */
			registerClassIdentifier(
					"org.locationtech.geowave.core.store.query.BasicQueryTest$ExampleDimensionOne",
					(short) 401);
			registerClassIdentifier(
					"org.locationtech.geowave.core.index.PersistenceUtilsTest$APersistable",
					(short) 402);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter",
					(short) 403);
			registerClassIdentifier(
					"org.locationtech.geowave.analytic.mapreduce.kmeans.TestObjectDataAdapter$1",
					(short) 404);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter",
					(short) 405);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$TestGeometryAdapter$1",
					(short) 406);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest$AnotherAdapter",
					(short) 407);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter",
					(short) 408);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.query.AccumuloRangeQueryTest$TestGeometryAdapter$1",
					(short) 409);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter",
					(short) 410);
			registerClassIdentifier(
					"org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest$TestGeometryAdapter$1",
					(short) 411);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$SummingMergeStrategy",
					(short) 412);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$SumAndAveragingMergeStrategy",
					(short) 413);
			registerClassIdentifier(
					"org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT$MergeCounter",
					(short) 414);
		}
		catch (Exception ex) {
			LOGGER.error(
					"Error registering class identifier: " + ex.getLocalizedMessage(),
					ex);
		}
	}
}
