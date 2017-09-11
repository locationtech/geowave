package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import javax.media.jai.PlanarImage;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.wdtinc.mapbox_vector_tile.adapt.jts.MvtReader;
import com.wdtinc.mapbox_vector_tile.adapt.jts.TagKeyValueMapConverter;

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
				PlanarImage.getDefaultColorModel(
						DataBuffer.TYPE_INT,
						2),
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

	public Raster createRaster() {
		WritableRaster raster;

		raster = Raster.createPackedRaster(
				DataBuffer.TYPE_INT,
				256,
				256,
				2,
				16,
				null);

		int density;
		int count;
		int xValue = 0;
		int yValue = 0;

		InputStream inputStream = (InputStream) getInput();
		GeometryFactory geomFactory = new GeometryFactory();
		TagKeyValueMapConverter converter = new TagKeyValueMapConverter();

		try {
			List<Geometry> geometryList = MvtReader.loadMvt(
					inputStream,
					geomFactory,
					converter);
			geometryList.get(
					0).getCoordinates();
			geometryList.get(
					0).getUserData();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// calculate resolution level

		/*
		 * for (Layer layer : tile.getLayersList()) { for (Feature feature :
		 * layer.getFeaturesList()) { // TODO calculate and set the grid values
		 * } density = (int) layer.getValues( 0).getIntValue(); count = (int)
		 * layer.getValues( 1).getIntValue();
		 * 
		 * // Count is band 0, density is band 1 raster.setSample( xValue,
		 * yValue, 0, count); raster.setSample( xValue, yValue, 1, density); }
		 */

		return raster;
	}

}
