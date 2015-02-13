package mil.nga.giat.geowave.vector.query.row;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.vector.plugin.GeoWaveGTDataStore;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * This encapsulates mappings of pixels on a spatially referenced image to/from
 * row IDs. It can be given a pixelSize to track larger grids of pixels for
 * performance purposes rather than tracking each individual pixel (eg. a
 * pixelSize of 4 would break the image into a grid of 4x4 pixel cells and map
 * row IDs to/from the larger cells). Also it maintains the painted ranges of
 * Accumulo row IDs which is important to determine if the current row has been
 * marked as painted and should be skipped. It does mark any row IDs outside of
 * the image's ReferencedEnvelope as painted.
 * 
 */
public class BasicRowIdStore
{
	private static final Logger LOGGER = Logger.getLogger(BasicRowIdStore.class);
	private final int height;
	private final Map<Integer, Key> pixelToRowIdMap = new HashMap<Integer, Key>();
	private final TreeMap<Key, Set<Integer>> rowIdToPixelsMap = new TreeMap<Key, Set<Integer>>();
	private List<Range> paintedPixelRanges = new ArrayList<Range>();
	private final Integer pixelSize;
	protected Key currentRow;
	protected Key lastRow;

	public BasicRowIdStore(
			final int width,
			final int height,
			final NumericIndexStrategy indexStrategy,
			final ReferencedEnvelope env,
			final Integer pixelSize )
			throws NoninvertibleTransformException {
		this.height = height;
		this.pixelSize = (pixelSize != null) ? Math.max(
				1,
				pixelSize) : null;
		final AffineTransform worldToScreen = RendererUtilities.worldToScreenTransform(
				env,
				new Rectangle(
						width,
						height));

		MathTransform worldToData = null;
		try {
			worldToData = CRS.findMathTransform(
					env.getCoordinateReferenceSystem(),
					GeoWaveGTDataStore.DEFAULT_CRS);
		}
		catch (final FactoryException e) {
			LOGGER.warn(
					"Unable to transform from map projection to default data projection (EPSG:4326)",
					e);
		}
		int increment = 1;
		if (pixelSize != null) {
			increment = pixelSize;
		}
		for (int x = 0; x < width; x += increment) {
			for (int y = 0; y < height; y += increment) {
				final Key rowIdKey = getMinRowId(
						x,
						y,
						increment,
						indexStrategy,
						worldToScreen,
						worldToData);
				if (rowIdKey == null) {
					LOGGER.warn("Row ID cannot be determined for pixel [x=" + x + ", y=" + y);
					continue;
				}
				final int pixelIndex = getPixelIndex(
						x,
						y);
				pixelToRowIdMap.put(
						pixelIndex,
						rowIdKey);
				Set<Integer> pixels = rowIdToPixelsMap.get(rowIdKey);
				if (pixels == null) {
					pixels = new HashSet<Integer>();
					rowIdToPixelsMap.put(
							rowIdKey,
							pixels);
				}
				pixels.add(pixelIndex);

			}
		}
		// mark all rows prior to the first row ID as "painted"
		paintedPixelRanges.add(new Range(
				null,
				rowIdToPixelsMap.firstKey()));
	}

	protected Key getMinRowId(
			final int x,
			final int y,
			final int yOffset,
			final NumericIndexStrategy indexStrategy,
			final AffineTransform worldToScreen,
			final MathTransform worldToData )
			throws NoninvertibleTransformException,
			MismatchedDimensionException {
		Key minRowId = null;
		// add one to the pixel in height because pixel space starts at
		// the top and goes down so to get the lower left spatial
		// coordinate for a pixel you want the [x, y+1] in pixel space
		final Point2D pixel = new Point2D.Double(
				x,
				y + yOffset);
		final Point2D mapProjPt = worldToScreen.inverseTransform(
				pixel,
				null);
		final DirectPosition2D lonLat = new DirectPosition2D();
		if (worldToData != null) {
			try {
				worldToData.transform(
						new DirectPosition2D(
								mapProjPt),
						lonLat);
			}
			catch (final TransformException e) {
				LOGGER.warn(
						"Unable to transform point from map projection to default data projection (EPSG:4326)",
						e);
			}
		}
		else {
			lonLat.setLocation(mapProjPt);
		}
		final List<ByteArrayId> rowIdsForPixel = indexStrategy.getInsertionIds(GeometryUtils.basicConstraintsFromPoint(
				lonLat.getY(),
				lonLat.getX()).getIndexConstraints(
				indexStrategy));
		// this should be of size one if its a simple spatial index
		if (!rowIdsForPixel.isEmpty()) {
			final ByteArrayId rowId = rowIdsForPixel.get(0);
			final Key rowIdKey = new Key(
					new Text(
							rowId.getBytes()));
			if (minRowId == null || (minRowId.compareTo(rowIdKey) > 0)) {
				minRowId = rowIdKey;
			}
		}
		return minRowId;
	}

	protected int getPixelIndex(
			final int x,
			final int y ) {
		if ((pixelSize != null) && (pixelSize != 1)) {
			return ((x / pixelSize) * (int) Math.ceil((double) height / (double) pixelSize)) + (y / pixelSize);
		}
		return (x * height) + y;
	}

	public Key getNextRow(
			final Key currentRow ) {
		final Iterator<Key> rowIdIt = rowIdToPixelsMap.keySet().iterator();
		while (rowIdIt.hasNext()) {
			final Key rowId = rowIdIt.next();
			if (rowId.compareTo(currentRow) < 0) {
				rowIdIt.remove();
			}
			else {
				return rowId;
			}
		}
		return null;
	}

	public boolean isPainted(
			final Key rowId ) {
		for (final Range range : paintedPixelRanges) {
			if (range.contains(rowId)) {
				return true;
			}
		}
		if ((currentRow != null) && ((lastRow == null) || !lastRow.equals(currentRow))) {
			// this case occurs if the pixel has already been decimated, to
			// prevent over sampling when the rows do not match the pixels

			// tracking the last row just is a performance improvement to ensure
			// this doesn't happen many times continuously (typically a single
			// row/feature results in many pixels being painted, we only need to
			// perform this decimation once per row)
			final Set<Integer> pixelIds = rowIdToPixelsMap.get(currentRow);

			lastRow = currentRow;
			if ((pixelIds != null) && (pixelIds.size() > ((pixelSize != null) ? pixelSize : 1))) {
				// this row ID spans multiple pixels, and more pixels than our
				// threshold on pixel size

				// we cannot decimate
				return false;
			}
			// otherwise decimate the range of row IDs spanning the pixel for
			// the current row
			final Key start = rowIdToPixelsMap.floorKey(currentRow);
			final Key stop = rowIdToPixelsMap.higherKey(currentRow);
			final Range decimatedRange = new Range(
					start,
					true,
					stop,
					false);
			incorporatePaintedPixel(decimatedRange);
		}
		return false;
	}

	public void notifyPixelPainted(
			final int x,
			final int y,
			final boolean shouldDecimate ) {
		// pass all final decisions to decimate into this function so that if
		// counts need to be applied, they can be applied here
		if (shouldDecimate) {
			final int pixelIndex = getPixelIndex(
					x,
					y);
			decimate(pixelIndex);
		}
	}

	protected boolean decimate(
			final int pixelIndex ) {
		final Key rowId = pixelToRowIdMap.remove(pixelIndex);
		if (rowId != null) {
			final Set<Integer> pixelIds = rowIdToPixelsMap.get(rowId);
			if (pixelIds != null) {
				pixelIds.remove(pixelIndex);
				if (pixelIds.isEmpty()) {
					rowIdToPixelsMap.remove(rowId);
					// mark all rows between this pixel and the next pixel
					final Range decimatedRange = new Range(
							rowId,
							true,
							rowIdToPixelsMap.higherKey(rowId), // if this is
																// null it
																// will
							// represent positive infinite
							false);
					incorporatePaintedPixel(decimatedRange);
					return true;
				}
			}
		}
		return false;
	}

	protected void incorporatePaintedPixel(
			final Range newRange ) {
		paintedPixelRanges.add(newRange);
		paintedPixelRanges = Range.mergeOverlapping(paintedPixelRanges);
	}

	public boolean setCurrentRow(
			final Key currentRow ) {
		this.currentRow = currentRow;
		return true;
	}
}
