package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
import java.awt.image.RenderedImage;

import org.geoserver.wms.map.RenderedImageMapOutputFormat;
import org.geotools.renderer.lite.DistributedRenderer;
import org.geotools.renderer.lite.StreamingRenderer;

public class DistributedRenderMapOutputFormat extends
		RenderedImageMapOutputFormat
{
	private final DistributedRenderOptions options;
	private DistributedRenderer currentRenderer;
	private BufferedImage currentImage;

	public DistributedRenderMapOutputFormat(
			final DistributedRenderOptions options ) {
		super(
				new DistributedRenderWMSFacade(
						options));
		this.options = options;
	}

	@Override
	protected StreamingRenderer buildRenderer() {
		currentRenderer = new DistributedRenderer(
				options);
		return currentRenderer;
	}

	public void stopRendering() {
		if (currentRenderer != null) {
			currentRenderer.stopRendering();
		}
	}

	@Override
	protected RenderedImage prepareImage(
			final int width,
			final int height,
			final IndexColorModel palette,
			final boolean transparent ) {
		currentImage = (BufferedImage) super.prepareImage(
				width,
				height,
				palette,
				transparent);
		return currentImage;
	}

	public DistributedRenderResult getDistributedRenderResult() {
		return currentRenderer.getResult(currentImage);
	}

}
