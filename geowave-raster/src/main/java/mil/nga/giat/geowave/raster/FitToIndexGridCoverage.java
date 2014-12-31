package mil.nga.giat.geowave.raster;

import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;

import org.geotools.geometry.GeneralEnvelope;
import org.opengis.coverage.CannotEvaluateException;
import org.opengis.coverage.PointOutsideCoverageException;
import org.opengis.coverage.SampleDimension;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.Record;
import org.opengis.util.RecordType;

import com.vividsolutions.jts.geom.Geometry;

public class FitToIndexGridCoverage implements
		GridCoverage
{
	private final GridCoverage gridCoverage;
	private final ByteArrayId insertionId;
	private final Resolution resolution;
	private final Envelope originalEnvelope;
	private final Geometry footprintWorldGeometry;
	private final Geometry footprintScreenGeometry;

	public FitToIndexGridCoverage(
			final GridCoverage gridCoverage,
			final ByteArrayId insertionId,
			final Resolution resolution,
			final Envelope originalEnvelope,
			final Geometry footprintWorldGeometry,
			final Geometry footprintScreenGeometry ) {
		this.gridCoverage = gridCoverage;
		this.insertionId = insertionId;
		this.resolution = resolution;
		this.originalEnvelope = originalEnvelope;
		this.footprintWorldGeometry = footprintWorldGeometry;
		this.footprintScreenGeometry = footprintScreenGeometry;
	}

	public Geometry getFootprintWorldGeometry() {
		return footprintWorldGeometry;
	}

	public Geometry getFootprintScreenGeometry() {
		return footprintScreenGeometry;
	}

	public ByteArrayId getInsertionId() {
		return insertionId;
	}

	public Resolution getResolution() {
		return resolution;
	}

	public GridCoverage getOriginalCoverage() {
		return gridCoverage;
	}

	@Override
	public boolean isDataEditable() {
		return gridCoverage.isDataEditable();
	}

	@Override
	public GridGeometry getGridGeometry() {
		return gridCoverage.getGridGeometry();
	}

	@Override
	public int[] getOptimalDataBlockSizes() {
		return gridCoverage.getOptimalDataBlockSizes();
	}

	@Override
	public int getNumOverviews() {
		return gridCoverage.getNumOverviews();
	}

	@Override
	public GridGeometry getOverviewGridGeometry(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getOverviewGridGeometry(index);
	}

	@Override
	public GridCoverage getOverview(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getOverview(index);
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return gridCoverage.getCoordinateReferenceSystem();
	}

	@Override
	public Envelope getEnvelope() {
		final int dimensions = originalEnvelope.getDimension();
		final Envelope indexedEnvelope = gridCoverage.getEnvelope();
		final double[] minDP = new double[dimensions];
		final double[] maxDP = new double[dimensions];
		for (int d = 0; d < dimensions; d++) {
			// to perform the intersection of the original envelope and the
			// indexed envelope, use the max of the mins per dimension and the
			// min of the maxes
			minDP[d] = Math.max(
					originalEnvelope.getMinimum(d),
					indexedEnvelope.getMinimum(d));
			maxDP[d] = Math.min(
					originalEnvelope.getMaximum(d),
					indexedEnvelope.getMaximum(d));
		}
		return new GeneralEnvelope(
				minDP,
				maxDP);
	}

	@Override
	public List<GridCoverage> getSources() {
		return gridCoverage.getSources();
	}

	@Override
	public RecordType getRangeType() {
		return gridCoverage.getRangeType();
	}

	@Override
	public Set<Record> evaluate(
			final DirectPosition p,
			final Collection<String> list )
			throws PointOutsideCoverageException,
			CannotEvaluateException {
		return gridCoverage.evaluate(
				p,
				list);
	}

	@Override
	public RenderedImage getRenderedImage() {
		return gridCoverage.getRenderedImage();
	}

	@Override
	public Object evaluate(
			final DirectPosition point )
			throws PointOutsideCoverageException,
			CannotEvaluateException {
		return gridCoverage.evaluate(point);
	}

	@Override
	public boolean[] evaluate(
			final DirectPosition point,
			final boolean[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public byte[] evaluate(
			final DirectPosition point,
			final byte[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public int[] evaluate(
			final DirectPosition point,
			final int[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public float[] evaluate(
			final DirectPosition point,
			final float[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public double[] evaluate(
			final DirectPosition point,
			final double[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public int getNumSampleDimensions() {
		return gridCoverage.getNumSampleDimensions();
	}

	@Override
	public SampleDimension getSampleDimension(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getSampleDimension(index);
	}

	@Override
	public RenderableImage getRenderableImage(
			final int xAxis,
			final int yAxis )
			throws UnsupportedOperationException,
			IndexOutOfBoundsException {
		return gridCoverage.getRenderableImage(
				xAxis,
				yAxis);
	}

}
