package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;

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
		Raster readRaster = readRaster(
				imageIndex,
				param);
		return null;
	}

	public Raster readRaster(
			final int imageIndex,
			ImageReadParam param )
			throws IOException {
		return null;

	}

}
