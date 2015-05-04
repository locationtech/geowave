package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.Serializer;
import javax.media.jai.remote.SerializerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.vector.wms.DelayedBackbufferGraphic;
import mil.nga.giat.geowave.core.index.Persistable;

import org.apache.log4j.Logger;
import org.geoserver.wms.map.ImageUtils;
import org.geotools.renderer.label.LabelCacheImpl.LabelRenderingMode;
import org.geotools.renderer.lite.StyledShapePainter;
import org.geotools.renderer.lite.StyledShapePainter.TextureAnchorKey;
import org.geotools.renderer.style.SLDStyleFactory;

import com.sun.media.jai.rmi.SerializableStateImpl;

/**
 * This class persists and encapsulates all of the main rendering information
 * for a layer (any render configuration that is not self-contained within a
 * single style). It contains the master image to which all labels will be
 * rendered on.
 * 
 */
public class ServerRenderOptions implements
		Persistable
{
	private final static Logger LOGGER = Logger.getLogger(ServerRenderOptions.class);
	private static final AtomicBoolean serializerRegistered = new AtomicBoolean(
			false);

	protected RenderingHints renderingHints;
	protected Color bgColor;
	protected boolean useAlpha;
	protected LabelRenderingMode labelRenderingMode;
	/**
	 * The meta buffer for the current layer
	 */
	protected int metaBuffer;
	protected double scaleDenominator;
	protected double angle;
	protected boolean clone;
	protected boolean continuousMapWrapping;
	protected boolean advancedProjectionHandlingEnabled;

	protected boolean vectorRenderingEnabled;
	protected boolean lineOptimizationEnabled;

	protected Graphics2D masterGraphics;
	protected DelayedBackbufferGraphic labelGraphics;
	protected BufferedImage masterImage;
	/** Factory that will resolve symbolizers into rendered styles */
	protected SLDStyleFactory styleFactory;

	protected ServerRenderOptions() {}

	public ServerRenderOptions(
			final RenderingHints renderingHints,
			final Color bgColor,
			final LabelRenderingMode labelRenderingMode,
			final int metaBuffer,
			final double scaleDenominator,
			final double angle,
			final boolean useAlpha,
			final boolean continuousMapWrapping,
			final boolean advancedProjectionHandlingEnabled,
			final boolean clone,
			final boolean vectorRenderingEnabled,
			final boolean lineOptimizationEnabled ) {
		this.renderingHints = renderingHints;
		this.bgColor = bgColor;
		this.labelRenderingMode = labelRenderingMode;
		this.metaBuffer = metaBuffer;
		this.scaleDenominator = scaleDenominator;
		this.angle = angle;
		this.useAlpha = useAlpha;
		this.continuousMapWrapping = continuousMapWrapping;
		this.advancedProjectionHandlingEnabled = advancedProjectionHandlingEnabled;
		this.clone = clone;
		this.vectorRenderingEnabled = vectorRenderingEnabled;
		this.lineOptimizationEnabled = lineOptimizationEnabled;
	}

	protected void init(
			final ServerPaintArea paintArea ) {
		masterImage = prepareImage(
				paintArea.getWidth(),
				paintArea.getHeight(),
				useAlpha);
		masterGraphics = ImageUtils.prepareTransparency(
				useAlpha,
				bgColor,
				masterImage,
				null);
		if (renderingHints != null) {
			masterGraphics.setRenderingHints(renderingHints);
		}
		styleFactory = new SLDStyleFactory();
		styleFactory.setRenderingHints(renderingHints);
		styleFactory.setVectorRenderingEnabled(vectorRenderingEnabled);
		styleFactory.setLineOptimizationEnabled(lineOptimizationEnabled);
		masterGraphics.setClip(paintArea.getArea());
		labelGraphics = new DelayedBackbufferGraphic(
				masterGraphics,
				paintArea.getArea());
	}

	/**
	 * Sets up a {@link BufferedImage#TYPE_4BYTE_ABGR} if the paletteInverter is
	 * not provided, or a indexed image otherwise. Subclasses may override this
	 * method should they need a special kind of image
	 * 
	 * @param width
	 * @param height
	 * @param paletteInverter
	 * @return
	 */
	protected BufferedImage prepareImage(
			final int width,
			final int height,
			final boolean transparent ) {
		return ImageUtils.createImage(
				width,
				height,
				null,
				transparent);
	}

	protected RenderedMaster getRenderedMaster(
			final List<ServerFeatureStyle> styles ) {
		return new RenderedMaster(
				styles,
				labelGraphics.getImage());
	}

	@Override
	public byte[] toBinary() {
		registerSerializers();

		final SerializableState serializableRenderingHints = SerializerFactory.getState(renderingHints);
		byte[] renderHintsBinary = new byte[0];
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableRenderingHints);
			renderHintsBinary = baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize rendering hints",
					e);
		}
		final ByteBuffer buf = ByteBuffer.allocate(renderHintsBinary.length + 39);
		buf.putInt(renderHintsBinary.length);
		buf.put(renderHintsBinary);
		buf.putInt(bgColor.getRGB());
		buf.put((byte) (useAlpha ? 1 : 0));
		buf.put((byte) (continuousMapWrapping ? 1 : 0));
		buf.put((byte) (advancedProjectionHandlingEnabled ? 1 : 0));
		buf.putInt(labelRenderingMode.ordinal());
		buf.putInt(metaBuffer);
		buf.putDouble(scaleDenominator);
		buf.putDouble(angle);
		buf.put((byte) (clone ? 1 : 0));
		buf.put((byte) (vectorRenderingEnabled ? 1 : 0));
		buf.put((byte) (lineOptimizationEnabled ? 1 : 0));
		return buf.array();
	}

	@SuppressFBWarnings(value = {
		"JLM_JSR166_UTILCONCURRENT_MONITORENTER"
	}, justification = "lock ensures transactional atomicity")
	private void registerSerializers() {
		synchronized (serializerRegistered) {
			if (!serializerRegistered.get()) {
				SerializerFactory.registerSerializer(new TextureAnchorKeySerializer());
				SerializerFactory.registerSerializer(new Point2dSerializer());
				serializerRegistered.set(true);
			}
		}
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int renderHintsBinaryLength = buf.getInt();
		final byte[] renderHintsBinary = new byte[renderHintsBinaryLength];
		buf.get(renderHintsBinary);
		renderingHints = null;
		try {
			registerSerializers();
			final ByteArrayInputStream bais = new ByteArrayInputStream(
					renderHintsBinary);
			final ObjectInputStream ois = new ObjectInputStream(
					bais);
			final Object o = ois.readObject();
			if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof RenderingHints)) {
				renderingHints = (RenderingHints) ((SerializableState) o).getObject();
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize rendering hints",
					e);
		}
		bgColor = new Color(
				buf.getInt());
		useAlpha = buf.get() > 0;
		continuousMapWrapping = buf.get() > 0;
		advancedProjectionHandlingEnabled = buf.get() > 0;
		labelRenderingMode = LabelRenderingMode.values()[buf.getInt()];
		metaBuffer = buf.getInt();
		scaleDenominator = buf.getDouble();
		angle = buf.getDouble();
		clone = buf.get() > 0;

		vectorRenderingEnabled = buf.get() > 0;
		lineOptimizationEnabled = buf.get() > 0;
	}

	private static class TextureAnchorKeySerializer implements
			Serializer
	{

		@Override
		public SerializableState getState(
				final Object obj,
				final RenderingHints renderingHints ) {
			return new TextureAnchorKeySerializableState(
					TextureAnchorKey.class,
					obj,
					renderingHints);
		}

		@Override
		public Class getSupportedClass() {
			return TextureAnchorKey.class;
		}

		@Override
		public boolean permitsSubclasses() {
			return false;
		}

	}

	private static class TextureAnchorKeySerializableState extends
			SerializableStateImpl
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		protected TextureAnchorKeySerializableState(
				final Class arg0,
				final Object arg1,
				final RenderingHints arg2 ) {
			super(
					arg0,
					arg1,
					arg2);
		}

		private void writeObject(
				final ObjectOutputStream out )
				throws IOException {

		}

		private void readObject(
				final ObjectInputStream in )
				throws IOException,
				ClassNotFoundException {
			theObject = StyledShapePainter.TEXTURE_ANCHOR_HINT_KEY;
		}
	}

	private static class Point2dSerializer implements
			Serializer
	{

		@Override
		public SerializableState getState(
				final Object obj,
				final RenderingHints renderingHints ) {
			return new Point2DSerializableState(
					Point2D.Double.class,
					obj,
					renderingHints);
		}

		@Override
		public Class getSupportedClass() {
			return Point2D.Double.class;
		}

		@Override
		public boolean permitsSubclasses() {
			return false;
		}

	}

	private static class Point2DSerializableState extends
			SerializableStateImpl
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		protected Point2DSerializableState(
				final Class arg0,
				final Object arg1,
				final RenderingHints arg2 ) {
			super(
					arg0,
					arg1,
					arg2);
		}

		private void writeObject(
				final ObjectOutputStream out )
				throws IOException {
			out.writeDouble(((Point2D) theObject).getX());
			out.writeDouble(((Point2D) theObject).getY());
		}

		private void readObject(
				final ObjectInputStream in )
				throws IOException,
				ClassNotFoundException {
			theObject = new Point2D.Double(
					in.readDouble(),
					in.readDouble());

		}
	}
}
