package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.IndexColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.ImageInputStream;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagKeyValueMapConverter;

public class ProtobufReader extends
		ImageReader
{

	protected ProtobufReader(
			final ImageReaderSpi originatingProvider ) {
		super(
				originatingProvider);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getNumImages(
			final boolean allowSearch )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getWidth(
			final int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHeight(
			final int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterator<ImageTypeSpecifier> getImageTypes(
			final int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IIOMetadata getStreamMetadata()
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IIOMetadata getImageMetadata(
			final int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BufferedImage read(
			final int imageIndex,
			final ImageReadParam param )
			throws IOException {

		return new BufferedImage(
				grayColorModel(
						256,
						13,
						256),
				(WritableRaster) createRaster(),
				false,
				null);

	}

	@Override
	public Raster readRaster(
			final int imageIndex,
			final ImageReadParam param )
			throws IOException {
		final ImageInputStream inputStream = (ImageInputStream) getInput();
		return null;

	}

	public Raster createRaster()
			throws IOException {
		WritableRaster raster;

		raster = Raster.createPackedRaster(
				DataBuffer.TYPE_INT,
				256,
				256,
				1,
				16,
				null);
		
		int count;
		int xValue = 0;
		int yValue = 0;

		final FileCacheImageInputStream imageInputStream = (FileCacheImageInputStream) getInput();
		final InputStream inputStream = new GZIPInputStream(
				new ImageInputStreamWrapper(
						imageInputStream));

		final GeometryFactory geomFactory = new GeometryFactory();
		final TagKeyValueMapConverter converter = new TagKeyValueMapConverter();

		try {
			final List<Geometry> geometryList = MvtReader.loadMvt(
					inputStream,
					geomFactory,
					converter,
					// with the default, ring_classifier v2.1, it always return
					// empty geometries
					MvtReader.RING_CLASSIFIER_V1);
			if (geometryList.isEmpty()) {
				return raster;
			}
			final double[] bounds = getBounds(
					geometryList.get(
							0).getCoordinates());
			final double extentPointX = Math.abs(
					bounds[0])
					- Math.abs(
							bounds[1]);
			final double extentPointY = Math.abs(
					bounds[2])
					- Math.abs(
							bounds[3]);
			final double extentRasterX = 256 * extentPointX;
			final double extentRasterY = 256 * extentPointY;

			for (final Geometry geometry : geometryList) {
				final Coordinate loopCoordinate = geometry.getCoordinates()[0];

				xValue = (int) (extentRasterX % loopCoordinate.x);
				yValue = (int) (extentRasterY % loopCoordinate.y);
				count = 9; // TODO get count from geometry.getUserData()

				raster.setSample(
						xValue,
						yValue,
						0,
						count);

			}

		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return raster;
	}

	public double[] getBounds(
			final Coordinate[] coordinates ) {
		final double[] bounds = new double[4];
		// minx = 0, maxx = 1, miny = 2. maxy = 3

		bounds[0] = coordinates[2].x;
		bounds[2] = coordinates[2].y;
		bounds[1] = coordinates[3].x;
		bounds[3] = coordinates[3].y;

		return bounds;
	}

	public IndexColorModel grayColorModel(
			final int window,
			final float level,
			final int maxval ) {
		int length = window;
		if (maxval > window) {
			length = maxval;
		}

		final byte[] r = new byte[length];
		final byte[] g = new byte[length];
		final byte[] b = new byte[length];

		for (int i = 0; i < length; i++) {
			int val = Math.round(
					(255 / (float) window) * ((i - level) + (window * 0.5f)));
			if (val > 255) {
				val = 255;
			}
			if (val < 0) {
				val = 0;
			}
			r[i] = (byte) val;
			g[i] = (byte) val;
			b[i] = (byte) val;
		}
		return (new IndexColorModel(
				16,
				length,
				r,
				g,
				b));
	}

	private static class ImageInputStreamWrapper extends
			InputStream
	{

		private final ImageInputStream imageInputStream;

		public ImageInputStreamWrapper(
				final ImageInputStream is ) {
			imageInputStream = is;
		}

		@Override
		public int read()
				throws IOException {
			return imageInputStream.read();
		}

		@Override
		public int read(
				final byte[] b,
				final int off,
				final int len )
				throws IOException {
			return imageInputStream.read(
					b,
					off,
					len);
		}

	}
}
