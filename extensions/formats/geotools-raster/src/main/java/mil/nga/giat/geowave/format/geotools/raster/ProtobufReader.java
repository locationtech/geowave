package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DirectColorModel;
import java.awt.image.IndexColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.ImageInputStream;
import javax.media.jai.PlanarImage;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagKeyValueMapConverter;

import mil.nga.giat.geowave.core.index.FloatCompareUtils;

/*import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos;
 import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile;
 import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Feature;
 import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Layer;
 import mil.nga.giat.geowave.format.geotools.raster.protobuf.VectorTileProtos.Tile.Value;*/

public class ProtobufReader extends
		ImageReader
{

	protected ProtobufReader(
			ImageReaderSpi originatingProvider ) {
		super(
				originatingProvider);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getNumImages(
			boolean allowSearch )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getWidth(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHeight(
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterator<ImageTypeSpecifier> getImageTypes(
			int imageIndex )
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
			int imageIndex )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BufferedImage read(
			int imageIndex,
			ImageReadParam param )
			throws IOException {
		
		return new BufferedImage(
				grayColorModel(256,13,256),
				(WritableRaster) createRaster(),
				false,
				null);

	}

	public Raster readRaster(
			final int imageIndex,
			ImageReadParam param )
			throws IOException {
		ImageInputStream inputStream = (ImageInputStream) getInput();
		return null;

	}

	public Raster createRaster() throws IOException {
		WritableRaster raster;

		raster = Raster.createPackedRaster(
				DataBuffer.TYPE_INT,
				256,
				256,
				1,
				16,
				null);

		int density;
		int count;
		int xValue = 0;
		int yValue = 0;

		FileCacheImageInputStream imageInputStream = (FileCacheImageInputStream) getInput();
		ImageInputStreamWrapper inputStream = new ImageInputStreamWrapper(imageInputStream);
		
		GeometryFactory geomFactory = new GeometryFactory();
		TagKeyValueMapConverter converter = new TagKeyValueMapConverter();

		try {
			List<Geometry> geometryList = MvtReader.loadMvt(
					inputStream,
					geomFactory,
					converter);
			for (Geometry geometry : geometryList) {

				Coordinate coordinate = geometry.getCoordinates()[0];
				double[] bounds = getBounds(coordinate);
				// minx = 0, maxx = 1, miny = 2. maxy = 3

				double diffx = Math.abs(bounds[0]) - Math.abs(bounds[1]);
				double diffy = Math.abs(bounds[2]) - Math.abs(bounds[3]);

				// edge case for points with a 0 values
				if (FloatCompareUtils.checkDoublesEqual(
						bounds[0],
						coordinate.x)) {
					xValue = 0;
				}
				if (FloatCompareUtils.checkDoublesEqual(
						bounds[3],
						coordinate.y)) {
					yValue = 0;
				}
				xValue = (int) Math.round(diffx / Math.abs(coordinate.x) * 255);
				yValue = (int) Math.round(diffy / Math.abs(coordinate.y) * 255);

				// TODO get count and density from geometry.getUserData()
				// Setting placeholder values for now
				count = 9;
				density = 1;
				raster.setSample(
						xValue,
						yValue,
						0,
						count);
				/*raster.setSample(
						xValue,
						yValue,
						1,
						density);*/

			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		// calculate resolution level

		
		 /*for (Layer layer : tile.getLayersList()) { for (Feature feature :
		 layer.getFeaturesList()) { // TODO calculate and set the grid values
		 } density = (int) layer.getValues( 0).getIntValue(); count = (int)
		 layer.getValues( 1).getIntValue();
		 
		 // Count is band 0, density is band 1 raster.setSample( xValue,
		 yValue, 0, count); raster.setSample( xValue, yValue, 1, density); }*/
		 
		return raster;
	}

	public double[] getBounds(
			Coordinate coordinate ) {
		double[] bounds = new double[4];
		// TODO Calculate bounds based on coordinate

		return bounds;
	}
	
	public IndexColorModel grayColorModel(int window, float level, int maxval) {
	    int length = window;
	    if (maxval > window) {
	        length = maxval;
	    }

	    byte[] r = new byte[length];
	    byte[] g = new byte[length];
	    byte[] b = new byte[length];

	    for (int i = 0; i < length; i++) {
	        int val = Math.round((255 / (float) window) * ((float) i - level + (float) window * 0.5f));
	        if (val > 255) {
	            val = 255;
	        }
	        if (val < 0) {
	            val = 0;
	        }
	        r[ i] = (byte) val;
	        g[ i] = (byte) val;
	        b[ i] = (byte) val;
	    }
	    return (new IndexColorModel(16, length, r, g, b));
	}

}

class ImageInputStreamWrapper extends InputStream {

    private ImageInputStream imageInputStream;

    public ImageInputStreamWrapper(ImageInputStream is) {
        imageInputStream = is;
    }

    public int read() throws IOException {
        return imageInputStream.read();
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
        return imageInputStream.read(b, off, len);
    }

}
