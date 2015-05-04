package mil.nga.giat.geowave.adapter.vector.query;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
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
		AccumuloCqlConstraintsQuery
{
	protected static final String CARDINALITY_SKIPPING_ITERATOR_NAME = "CARDINALITY_SKIPPING_ITERATOR";
	protected static final int CARDINALITY_SKIPPING_ITERATOR_PRIORITY = 15;
	private final int width;
	private final int height;
	private final double pixelSize;
	private final ReferencedEnvelope envelope;

	public SpatialDecimationQuery(
			final Index index,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope,
			final String... authorizations ) {
		super(
				index,
				cqlFilter,
				dataAdapter,
				authorizations);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
	}

	public SpatialDecimationQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final int width,
			final int height,
			final double pixelSize,
			final Filter cqlFilter,
			final FeatureDataAdapter dataAdapter,
			final ReferencedEnvelope envelope,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				cqlFilter,
				dataAdapter,
				authorizations);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
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
			final ReferencedEnvelope envelope,
			final String... authorizations ) {
		super(
				index,
				constraints,
				queryFilters,
				cqlFilter,
				dataAdapter,
				authorizations);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
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
			final ReferencedEnvelope envelope,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				constraints,
				queryFilters,
				cqlFilter,
				dataAdapter,
				authorizations);
		this.width = width;
		this.height = height;
		this.envelope = envelope;
		this.pixelSize = pixelSize;
	}

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		super.addScanIteratorSettings(scanner);
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
				// log base 2 to determine number of bits
				final double cardinalityX = Math.log((spans[0] / 360)) / Math.log(2);
				final double cardinalityY = Math.log((spans[1] / 180)) / Math.log(2);
				final long combinedCardinality = Math.max(
						-(Math.round(cardinalityX + cardinalityY)),
						0) + 8;

				iteratorSettings.addOption(
						FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
						Integer.toString((int) combinedCardinality));
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
