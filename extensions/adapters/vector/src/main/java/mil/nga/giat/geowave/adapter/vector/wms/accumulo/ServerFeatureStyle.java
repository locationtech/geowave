package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import javax.xml.transform.TransformerException;

import mil.nga.giat.geowave.adapter.vector.wms.DelayedBackbufferGraphic;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.log4j.Logger;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.renderer.ScreenMap;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.NamedLayer;
import org.geotools.styling.Rule;
import org.geotools.styling.SLDParser;
import org.geotools.styling.SLDTransformer;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.geotools.styling.StyledLayer;
import org.geotools.styling.StyledLayerDescriptor;
import org.opengis.referencing.operation.MathTransform;

/**
 * This class persists and encapsulates the rules and tyle information
 * associated with a GeoTools SLD.
 * 
 */
public class ServerFeatureStyle implements
		Persistable
{
	private final static Logger LOGGER = Logger.getLogger(ServerFeatureStyle.class);
	protected String styleId;
	protected Rule[] ruleList;

	protected Rule[] elseRules;

	private boolean screenMapEnabled = false;
	private ScreenMap screenMap = null;
	protected DelayedBackbufferGraphic graphics;

	protected ServerFeatureStyle() {}

	public ServerFeatureStyle(
			final String styleId,
			final Rule[] ruleList,
			final Rule[] elseRules,
			final ScreenMap screenMap ) {
		this.styleId = styleId;
		this.ruleList = ruleList;
		this.elseRules = elseRules;
		setScreenMap(screenMap);
	}

	protected void init(
			final ServerPaintArea paintArea,
			final ServerRenderOptions options,
			final MathTransform transform,
			final double[] spans ) {
		if (screenMapEnabled) {
			screenMap = new ScreenMap(
					paintArea.getMinX() - options.metaBuffer,
					paintArea.getMinY() - options.metaBuffer,
					paintArea.getWidth() + (options.metaBuffer * 2),
					paintArea.getHeight() + (options.metaBuffer * 2));
			screenMap.setTransform(transform);
			if (spans != null) {
				screenMap.setSpans(
						spans[0],
						spans[1]);
			}
		}
		else {
			screenMap = null;
		}
		graphics = new DelayedBackbufferGraphic(
				options.masterGraphics,
				paintArea.getArea());
	}

	public ScreenMap getScreenMap() {
		return screenMap;
	}

	public void setScreenMap(
			final ScreenMap screenMap ) {
		if (screenMap == null) {
			screenMapEnabled = false;
		}
		else {
			screenMapEnabled = true;
		}
		this.screenMap = screenMap;
	}

	@Override
	public byte[] toBinary() {
		byte[] ruleListBinary, elseRulesBinary;
		final byte[] styleIdBinary = StringUtils.stringToBinary(styleId);
		if ((ruleList != null) && (ruleList.length > 0)) {
			ruleListBinary = encodeRules(ruleList);
		}
		else {
			ruleListBinary = new byte[0];
		}
		if ((elseRules != null) && (elseRules.length > 0)) {
			elseRulesBinary = encodeRules(elseRules);
		}
		else {
			elseRulesBinary = new byte[0];
		}

		final ByteBuffer buf = ByteBuffer.allocate(13 + ruleListBinary.length + elseRulesBinary.length + styleIdBinary.length);
		buf.putInt(ruleListBinary.length);
		buf.put(ruleListBinary);
		buf.putInt(elseRulesBinary.length);
		buf.put(elseRulesBinary);
		buf.putInt(styleIdBinary.length);
		buf.put(styleIdBinary);
		buf.put((byte) (screenMapEnabled ? 1 : 0));
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int ruleListBinaryLength = buf.getInt();
		if (ruleListBinaryLength > 0) {
			final byte[] ruleListBinary = new byte[ruleListBinaryLength];
			buf.get(ruleListBinary);
			ruleList = decodeRules(ruleListBinary);
		}
		else {
			ruleList = new Rule[] {};
		}
		final int elseRulesBinaryLength = buf.getInt();
		if (elseRulesBinaryLength > 0) {
			final byte[] elseRulesBinary = new byte[elseRulesBinaryLength];
			buf.get(elseRulesBinary);
			elseRules = decodeRules(elseRulesBinary);
		}
		else {
			elseRules = new Rule[] {};
		}
		final int styleIdBinaryLength = buf.getInt();
		final byte[] styleIdBinary = new byte[styleIdBinaryLength];
		buf.get(styleIdBinary);
		styleId = StringUtils.stringFromBinary(styleIdBinary);
		screenMapEnabled = buf.get() > 0;
	}

	private static byte[] encodeRules(
			final Rule[] rules ) {
		// all that we care about is the rules, so encode a minimal set of an
		// sld to represent a set of rules
		final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);
		final Style style = styleFactory.createStyle();
		final List<FeatureTypeStyle> fts = style.featureTypeStyles();
		fts.clear();
		fts.add(styleFactory.createFeatureTypeStyle(rules));
		final NamedLayer nl = styleFactory.createNamedLayer();
		nl.setName("");
		nl.addStyle(style);

		final StyledLayerDescriptor sld = styleFactory.createStyledLayerDescriptor();
		sld.setStyledLayers(new StyledLayer[] {
			nl
		});
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final SLDTransformer writer = new SLDTransformer();
		try {
			writer.transform(
					sld,
					baos);
		}
		catch (final TransformerException e) {
			LOGGER.warn(
					"Unable to create SLD from style",
					e);
		}
		return baos.toByteArray();
	}

	private static Rule[] decodeRules(
			final byte[] rulesBinary ) {
		final SLDParser parser = new SLDParser(
				CommonFactoryFinder.getStyleFactory(null),
				new ByteArrayInputStream(
						rulesBinary));
		final StyledLayerDescriptor sld = parser.parseSLD();
		final List<StyledLayer> layers = sld.layers();
		if ((layers != null) && !layers.isEmpty()) {
			// drill down to eventually get the rules from the sld
			for (final StyledLayer l : layers) {
				if (l instanceof NamedLayer) {
					final Style[] styles = ((NamedLayer) l).getStyles();
					if ((styles != null) && (styles.length > 0)) {
						for (final Style s : styles) {
							if (!s.featureTypeStyles().isEmpty()) {
								List<Rule> var = s.featureTypeStyles().get(
										0).rules();
								return var.toArray(new Rule[var.size()]);
							}
						}
					}
				}
			}
		}
		return new Rule[] {};
	}

	protected RenderedStyle getRenderedStyle() {
		return new RenderedStyle(
				styleId,
				graphics.getImage());
	}
}
