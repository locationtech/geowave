package mil.nga.giat.geowave.adapter.raster;

import java.awt.RenderingHints;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

import javax.media.jai.Histogram;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedImageAdapter;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.image.ImageWorker;

public class ImageWorkerPredefineStats extends
		ImageWorker
{

	public ImageWorkerPredefineStats() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ImageWorkerPredefineStats(
			File input )
			throws IOException {
		super(
				input);
	}

	public ImageWorkerPredefineStats(
			RenderedImage image ) {
		super(
				image);
	}

	public ImageWorkerPredefineStats(
			RenderingHints hints ) {
		super(
				hints);
	}

	public ImageWorkerPredefineStats setStats(
			Pair<String, Object>[] nameValuePairs ) {
		image = new RenderedImageAdapter(
				image);
		for (Pair<String, Object> pair : nameValuePairs) {
			((PlanarImage) (image)).setProperty(
					pair.getLeft(),
					pair.getRight());
		}
		return this;
	}

	public ImageWorkerPredefineStats setHistogram(
			Histogram histogram ) {
		image = new RenderedImageAdapter(
				image);
		((PlanarImage) (image)).setProperty(
				"histogram",
				histogram);
		return this;
	}

	public ImageWorkerPredefineStats setExtrema(
			double[][] extrema ) {
		image = new RenderedImageAdapter(
				image);
		((PlanarImage) (image)).setProperty(
				"extrema",
				extrema);
		return this;
	}
}
