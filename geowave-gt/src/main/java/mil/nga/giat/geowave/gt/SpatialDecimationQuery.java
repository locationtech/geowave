package mil.nga.giat.geowave.gt;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.accumulo.EntryIteratorWrapper;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.gt.query.CqlQueryFilterIterator;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;

/**
 * This class extends the capabilities of a simple constraints query to perform
 * decimation (smart pixel-based row skipping) within tablet servers. It also
 * supports CQL filters within the tablet servers and it will only skip when a
 * row passes the filters.
 * 
 */
public class SpatialDecimationQuery extends
		AccumuloConstraintsQuery
{
	protected static final String DECIMATION_ITERATOR_NAME = "GEOWAVE_DECIMATION_ITERATOR";
	protected static final String CARDINALITY_SKIPPING_ITERATOR_NAME = "CARDINALITY_SKIPPING_ITERATOR";
	protected static final int DECIMATION_ITERATOR_PRIORITY = 5;
	protected static final int CARDINALITY_SKIPPING_ITERATOR_PRIORITY = 15;
	private final int width;
	private final int height;
	private final double pixelSize;
	private final Filter cqlFilter;
	private final FeatureDataAdapter dataAdapter;
	private final ReferencedEnvelope envelope;

	public SpatialDecimationQuery(
			final Index index,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope ) {
		super(
				index);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public SpatialDecimationQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope ) {
		super(
				adapterIds,
				index);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public SpatialDecimationQuery(
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope ) {
		super(
				index,
				constraints,
				queryFilters);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public SpatialDecimationQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
		this.cqlFilter = cqlFilter;
		this.dataAdapter = dataAdapter;
	}

	public CloseableIterator<SimpleFeature> queryDecimate(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				-1);
		addScanIteratorSettings(scanner);
		// TODO implement limit
		return new CloseableIteratorWrapper<SimpleFeature>(
				new ScannerClosableWrapper(
						scanner),
				new EntryIteratorWrapper(
						adapterStore,
						index,
						scanner.iterator(),
						new FilterList<QueryFilter>(
								clientFilters)));
	}

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		if ((cqlFilter != null) && (dataAdapter != null)) {
			final IteratorSetting iteratorSettings = new IteratorSetting(
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_PRIORITY,
					CqlQueryFilterIterator.CQL_QUERY_ITERATOR_NAME,
					CqlQueryFilterIterator.class);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.CQL_FILTER,
					ECQL.toCQL(cqlFilter));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.DATA_ADAPTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(dataAdapter)));
			iteratorSettings.addOption(
					CqlQueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));

			final DistributableQueryFilter filterList = new DistributableFilterList(
					distributableFilters);
			iteratorSettings.addOption(
					CqlQueryFilterIterator.GEOWAVE_FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filterList)));
			scanner.addScanIterator(iteratorSettings);
		}
		else {
			super.addScanIteratorSettings(scanner);
		}
		// TODO for now let's forget about CRS, but we should do a transform to
		// 4326 if it isn't already in that CRS
		final double east = envelope.getMaxX();
		final double west = envelope.getMinX();
		final double north = envelope.getMaxY();
		final double south = envelope.getMinY();
		final IteratorSetting iteratorSettings = new IteratorSetting(
				SpatialDecimationQuery.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
				SpatialDecimationQuery.CARDINALITY_SKIPPING_ITERATOR_NAME,
				FixedCardinalitySkippingIterator.class);

		try {
			final AffineTransform worldToScreen = RendererUtilities.worldToScreenTransform(
					new ReferencedEnvelope(
							new Envelope(
									west,
									east,
									south,
									north),
							CRS.decode("EPSG:4326")),
					new Rectangle(
							width,
							height));
			final MathTransform2D fullTransform = (MathTransform2D) ProjectiveTransform.create(worldToScreen);
			// calculate spans
			try {
				final double[] spans = Decimator.computeGeneralizationDistances(
						fullTransform.inverse(),
						new Rectangle(
								width,
								height),
						pixelSize);
				final double cardinalityX = Math.log((spans[0] / 360)) / Math.log(2);
				final double cardinalityY = Math.log((spans[1] / 180)) / Math.log(2);
				final long combinedCardinality = Math.max(
						-(Math.round(cardinalityX + cardinalityY)),
						0) + 8;

				iteratorSettings.addOption(
						FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
						new Integer(
								(int) combinedCardinality).toString());
				scanner.addScanIterator(iteratorSettings);
			}
			catch (final TransformException e) {
				throw new IllegalArgumentException(
						"Unable to compute generalization distance",
						e);
			}
		}
		catch (MismatchedDimensionException | FactoryException e) {
			throw new IllegalArgumentException(
					"Unable to decode CRS EPSG:4326",
					e);
		}
	}
}
