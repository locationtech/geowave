/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.Color;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationNearest;
import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;
import javax.xml.transform.TransformerException;

import org.geoserver.wms.DefaultWebMapService;
import org.geoserver.wms.GetMapRequest;
import org.geoserver.wms.WMS;
import org.geoserver.wms.WMSMapContent;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.SLDParser;
import org.geotools.styling.SLDTransformer;
import org.geotools.styling.Style;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.adapter.vector.utils.FeatureGeometryUtils;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;

public class DistributedRenderOptions implements
		Persistable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DistributedRenderOptions.class);
	// it doesn't make sense to grab this from the context of the geoserver
	// settings, although it is unclear whether in distributed rendering this
	// should be enabled or disabled by default
	private final static boolean USE_GLOBAL_RENDER_POOL = true;

	private String antialias;
	private boolean continuousMapWrapping;
	private boolean advancedProjectionHandlingEnabled;
	private boolean optimizeLineWidth;
	private boolean transparent;
	private boolean isMetatile;
	private boolean kmlPlacemark;
	private boolean renderScaleMethodAccurate;
	private int mapWidth;
	private int mapHeight;
	private int buffer;
	private double angle;
	private IndexColorModel palette;
	private Color bgColor;
	private int maxRenderTime;
	private int maxErrors;
	private int maxFilters;
	private ReferencedEnvelope envelope;
	private int wmsIterpolationOrdinal;
	private List<Integer> interpolationOrdinals;

	private Style style;

	public DistributedRenderOptions() {}

	public DistributedRenderOptions(
			final WMS wms,
			final WMSMapContent mapContent,
			final Style style ) {
		optimizeLineWidth = DefaultWebMapService.isLineWidthOptimizationEnabled();
		maxFilters = DefaultWebMapService.getMaxFilterRules();

		transparent = mapContent.isTransparent();
		buffer = mapContent.getBuffer();
		angle = mapContent.getAngle();
		mapWidth = mapContent.getMapWidth();
		mapHeight = mapContent.getMapHeight();
		bgColor = mapContent.getBgColor();
		palette = mapContent.getPalette();
		renderScaleMethodAccurate = StreamingRenderer.SCALE_ACCURATE.equals(mapContent.getRendererScaleMethod());
		wmsIterpolationOrdinal = wms.getInterpolation().ordinal();
		maxErrors = wms.getMaxRenderingErrors();
		this.style = style;
		envelope = mapContent.getRenderingArea();

		final GetMapRequest request = mapContent.getRequest();
		final Object timeoutOption = request.getFormatOptions().get(
				"timeout");
		int localMaxRenderTime = 0;
		if (timeoutOption != null) {
			try {
				// local render time is in millis, while WMS max render time is
				// in seconds
				localMaxRenderTime = Integer.parseInt(timeoutOption.toString()) / 1000;
			}
			catch (final NumberFormatException e) {
				LOGGER.warn(
						"Could not parse format_option \"timeout\": " + timeoutOption,
						e);
			}
		}
		maxRenderTime = getMaxRenderTime(
				localMaxRenderTime,
				wms);
		isMetatile = request.isTiled() && (request.getTilesOrigin() != null);
		final Object antialiasObj = request.getFormatOptions().get(
				"antialias");
		if (antialiasObj != null) {
			antialias = antialiasObj.toString();
		}

		if (request.getFormatOptions().get(
				"kmplacemark") != null) {
			kmlPlacemark = ((Boolean) request.getFormatOptions().get(
					"kmplacemark")).booleanValue();
		}
		// turn on advanced projection handling
		advancedProjectionHandlingEnabled = wms.isAdvancedProjectionHandlingEnabled();
		final Object advancedProjectionObj = request.getFormatOptions().get(
				WMS.ADVANCED_PROJECTION_KEY);
		if ((advancedProjectionObj != null) && "false".equalsIgnoreCase(advancedProjectionObj.toString())) {
			advancedProjectionHandlingEnabled = false;
			continuousMapWrapping = false;
		}
		final Object mapWrappingObj = request.getFormatOptions().get(
				WMS.ADVANCED_PROJECTION_KEY);
		if ((mapWrappingObj != null) && "false".equalsIgnoreCase(mapWrappingObj.toString())) {
			continuousMapWrapping = false;
		}
		final List<Interpolation> interpolations = request.getInterpolations();
		if ((interpolations == null) || interpolations.isEmpty()) {
			interpolationOrdinals = Collections.emptyList();
		}
		else {
			interpolationOrdinals = Lists.transform(
					interpolations,
					new Function<Interpolation, Integer>() {

						@Override
						public Integer apply(
								final Interpolation input ) {
							if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							return Interpolation.INTERP_NEAREST;
						}
					});
		}
	}

	public int getMaxRenderTime(
			final int localMaxRenderTime,
			final WMS wms ) {
		final int wmsMaxRenderTime = wms.getMaxRenderingTime();

		if (wmsMaxRenderTime == 0) {
			maxRenderTime = localMaxRenderTime;
		}
		else if (localMaxRenderTime != 0) {
			maxRenderTime = Math.min(
					wmsMaxRenderTime,
					localMaxRenderTime);
		}
		else {
			maxRenderTime = wmsMaxRenderTime;
		}
		return maxRenderTime;
	}

	public boolean isOptimizeLineWidth() {
		return optimizeLineWidth;
	}

	public int getMaxErrors() {
		return maxErrors;
	}

	public void setMaxErrors(
			final int maxErrors ) {
		this.maxErrors = maxErrors;
	}

	public void setOptimizeLineWidth(
			final boolean optimizeLineWidth ) {
		this.optimizeLineWidth = optimizeLineWidth;
	}

	public List<Integer> getInterpolationOrdinals() {
		return interpolationOrdinals;
	}

	public List<Interpolation> getInterpolations() {
		if ((interpolationOrdinals != null) && !interpolationOrdinals.isEmpty()) {
			return Lists.transform(
					interpolationOrdinals,
					new Function<Integer, Interpolation>() {

						@Override
						public Interpolation apply(
								final Integer input ) {
							return Interpolation.getInstance(input);
						}
					});
		}
		return Collections.emptyList();
	}

	public void setInterpolationOrdinals(
			final List<Integer> interpolationOrdinals ) {
		this.interpolationOrdinals = interpolationOrdinals;
	}

	public static boolean isUseGlobalRenderPool() {
		return USE_GLOBAL_RENDER_POOL;
	}

	public Style getStyle() {
		return style;
	}

	public void setStyle(
			final Style style ) {
		this.style = style;
	}

	public int getWmsInterpolationOrdinal() {
		return wmsIterpolationOrdinal;
	}

	public void setWmsInterpolationOrdinal(
			final int wmsIterpolationOrdinal ) {
		this.wmsIterpolationOrdinal = wmsIterpolationOrdinal;
	}

	public int getMaxRenderTime() {
		return maxRenderTime;
	}

	public void setMaxRenderTime(
			final int maxRenderTime ) {
		this.maxRenderTime = maxRenderTime;
	}

	public boolean isRenderScaleMethodAccurate() {
		return renderScaleMethodAccurate;
	}

	public void setRenderScaleMethodAccurate(
			final boolean renderScaleMethodAccurate ) {
		this.renderScaleMethodAccurate = renderScaleMethodAccurate;
	}

	public int getBuffer() {
		return buffer;
	}

	public void setBuffer(
			final int buffer ) {
		this.buffer = buffer;
	}

	public void setPalette(
			final IndexColorModel palette ) {
		this.palette = palette;
	}

	public String getAntialias() {
		return antialias;
	}

	public void setAntialias(
			final String antialias ) {
		this.antialias = antialias;
	}

	public boolean isContinuousMapWrapping() {
		return continuousMapWrapping;
	}

	public void setContinuousMapWrapping(
			final boolean continuousMapWrapping ) {
		this.continuousMapWrapping = continuousMapWrapping;
	}

	public boolean isAdvancedProjectionHandlingEnabled() {
		return advancedProjectionHandlingEnabled;
	}

	public void setAdvancedProjectionHandlingEnabled(
			final boolean advancedProjectionHandlingEnabled ) {
		this.advancedProjectionHandlingEnabled = advancedProjectionHandlingEnabled;
	}

	public boolean isKmlPlacemark() {
		return kmlPlacemark;
	}

	public void setKmlPlacemark(
			final boolean kmlPlacemark ) {
		this.kmlPlacemark = kmlPlacemark;
	}

	public boolean isTransparent() {
		return transparent;
	}

	public void setTransparent(
			final boolean transparent ) {
		this.transparent = transparent;
	}

	public boolean isMetatile() {
		return isMetatile;
	}

	public void setMetatile(
			final boolean isMetatile ) {
		this.isMetatile = isMetatile;
	}

	public Color getBgColor() {
		return bgColor;
	}

	public void setBgColor(
			final Color bgColor ) {
		this.bgColor = bgColor;
	}

	public int getMapWidth() {
		return mapWidth;
	}

	public void setMapWidth(
			final int mapWidth ) {
		this.mapWidth = mapWidth;
	}

	public int getMapHeight() {
		return mapHeight;
	}

	public void setMapHeight(
			final int mapHeight ) {
		this.mapHeight = mapHeight;
	}

	public double getAngle() {
		return angle;
	}

	public void setAngle(
			final double angle ) {
		this.angle = angle;
	}

	public int getMaxFilters() {
		return maxFilters;
	}

	public void setMaxFilters(
			final int maxFilters ) {
		this.maxFilters = maxFilters;
	}

	public ReferencedEnvelope getEnvelope() {
		return envelope;
	}

	public void setEnvelope(
			final ReferencedEnvelope envelope ) {
		this.envelope = envelope;
	}

	public IndexColorModel getPalette() {
		return palette;
	}

	@Override
	public byte[] toBinary() {
		// combine booleans into a bitset
		final BitSet bitSet = new BitSet(
				15);
		bitSet.set(
				0,
				continuousMapWrapping);
		bitSet.set(
				1,
				advancedProjectionHandlingEnabled);
		bitSet.set(
				2,
				optimizeLineWidth);
		bitSet.set(
				3,
				transparent);
		bitSet.set(
				4,
				isMetatile);
		bitSet.set(
				5,
				kmlPlacemark);
		bitSet.set(
				6,
				renderScaleMethodAccurate);
		final boolean storeInterpolationOrdinals = ((interpolationOrdinals != null) && !interpolationOrdinals.isEmpty());
		bitSet.set(
				7,
				storeInterpolationOrdinals);
		bitSet.set(
				8,
				palette != null);
		bitSet.set(
				9,
				maxRenderTime > 0);
		bitSet.set(
				10,
				maxErrors > 0);
		bitSet.set(
				11,
				angle != 0);
		bitSet.set(
				12,
				buffer > 0);
		bitSet.set(
				13,
				bgColor != null);
		bitSet.set(
				14,
				style != null);
		final boolean storeCRS = !((envelope.getCoordinateReferenceSystem() == null) || GeometryUtils.DEFAULT_CRS
				.equals(envelope.getCoordinateReferenceSystem()));
		bitSet.set(
				15,
				storeCRS);

		final double minX = envelope.getMinX();
		final double minY = envelope.getMinY();
		final double maxX = envelope.getMaxX();
		final double maxY = envelope.getMaxY();
		// required bytes include 32 for envelope doubles,
		// 8 for map width and height ints, and 2 for the bitset, and 4 for
		// maxFilters
		int bufferSize = 46;

		final byte[] wktBinary;
		if (storeCRS) {
			final String wkt = envelope.getCoordinateReferenceSystem().toWKT();
			wktBinary = StringUtils.stringToBinary(wkt);
			bufferSize += (wktBinary.length + 4);
		}
		else {
			wktBinary = null;
		}
		if (storeInterpolationOrdinals) {
			bufferSize += (4 * interpolationOrdinals.size()) + 4;
		}

		final byte[] paletteBinary;
		if (palette != null) {
			final SerializableState serializableColorModel = SerializerFactory.getState(palette);
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				final ObjectOutputStream oos = new ObjectOutputStream(
						baos);
				oos.writeObject(serializableColorModel);
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to serialize sample model",
						e);
			}
			paletteBinary = baos.toByteArray();
			bufferSize += (paletteBinary.length + 4);
		}
		else {
			paletteBinary = null;
		}
		if (maxRenderTime > 0) {
			bufferSize += 4;
		}
		if (maxErrors > 0) {
			bufferSize += 4;
		}
		if (angle != 0) {
			bufferSize += 8;
		}
		if (buffer > 0) {
			bufferSize += 4;
		}
		if (bgColor != null) {
			bufferSize += 4;
		}

		final byte[] styleBinary;
		if (style != null) {
			final SLDTransformer transformer = new SLDTransformer();

			final ByteArrayOutputStream baos = new ByteArrayOutputStream();

			try {
				transformer.transform(
						new Style[] {
							style
						},
						baos);
			}
			catch (final TransformerException e) {
				LOGGER.warn(
						"Unable to create SLD from style",
						e);
			}
			styleBinary = baos.toByteArray();
			bufferSize += (styleBinary.length + 4);
		}
		else {
			styleBinary = null;
		}
		final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
		byteBuffer.put(bitSet.toByteArray());
		byteBuffer.putDouble(minX);
		byteBuffer.putDouble(minY);
		byteBuffer.putDouble(maxX);
		byteBuffer.putDouble(maxY);
		byteBuffer.putInt(mapWidth);
		byteBuffer.putInt(mapHeight);
		if (wktBinary != null) {
			byteBuffer.putInt(wktBinary.length);
			byteBuffer.put(wktBinary);
		}
		if (storeInterpolationOrdinals) {
			byteBuffer.putInt(interpolationOrdinals.size());
			for (final Integer interpOrd : interpolationOrdinals) {
				byteBuffer.putInt(interpOrd);
			}
		}
		if (paletteBinary != null) {
			byteBuffer.putInt(paletteBinary.length);
			byteBuffer.put(paletteBinary);
		}
		if (maxRenderTime > 0) {
			byteBuffer.putInt(maxRenderTime);
		}
		if (maxErrors > 0) {
			byteBuffer.putInt(maxErrors);
		}
		if (angle != 0) {
			byteBuffer.putDouble(angle);
		}
		if (buffer > 0) {
			byteBuffer.putInt(buffer);
		}
		if (bgColor != null) {
			byteBuffer.putInt(bgColor.getRGB());
		}
		if (styleBinary != null) {
			byteBuffer.putInt(styleBinary.length);
			byteBuffer.put(styleBinary);
		}
		return byteBuffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] bitSetBytes = new byte[2];
		buf.get(bitSetBytes);
		final BitSet bitSet = BitSet.valueOf(bitSetBytes);
		continuousMapWrapping = bitSet.get(0);
		advancedProjectionHandlingEnabled = bitSet.get(1);
		optimizeLineWidth = bitSet.get(2);
		transparent = bitSet.get(3);
		isMetatile = bitSet.get(4);
		kmlPlacemark = bitSet.get(5);
		renderScaleMethodAccurate = bitSet.get(6);
		final boolean interpolationOrdinalsStored = bitSet.get(7);
		final boolean paletteStored = bitSet.get(8);
		final boolean maxRenderTimeStored = bitSet.get(9);
		final boolean maxErrorsStored = bitSet.get(10);
		final boolean angleStored = bitSet.get(11);
		final boolean bufferStored = bitSet.get(12);
		final boolean bgColorStored = bitSet.get(13);
		final boolean styleStored = bitSet.get(14);
		final boolean crsStored = bitSet.get(15);
		CoordinateReferenceSystem crs;
		final double minX = buf.getDouble();
		final double minY = buf.getDouble();
		final double maxX = buf.getDouble();
		final double maxY = buf.getDouble();
		mapWidth = buf.getInt();
		mapHeight = buf.getInt();
		if (crsStored) {
			final byte[] wktBinary = new byte[buf.getInt()];
			buf.get(wktBinary);
			final String wkt = StringUtils.stringFromBinary(wktBinary);
			try {
				crs = CRS.parseWKT(wkt);
			}
			catch (final FactoryException e) {
				LOGGER.warn(
						"Unable to parse coordinate reference system",
						e);
				crs = GeometryUtils.DEFAULT_CRS;
			}
		}
		else {
			crs = GeometryUtils.DEFAULT_CRS;
		}
		envelope = new ReferencedEnvelope(
				minX,
				maxX,
				minY,
				maxY,
				crs);
		if (interpolationOrdinalsStored) {
			final int interpolationsLength = buf.getInt();
			interpolationOrdinals = new ArrayList<>(
					interpolationsLength);
			for (int i = 0; i < interpolationsLength; i++) {
				interpolationOrdinals.add(buf.getInt());
			}
		}
		else {
			interpolationOrdinals = Collections.emptyList();
		}
		if (paletteStored) {
			final byte[] colorModelBinary = new byte[buf.getInt()];
			buf.get(colorModelBinary);
			try {
				final ByteArrayInputStream bais = new ByteArrayInputStream(
						colorModelBinary);
				final ObjectInputStream ois = new ObjectInputStream(
						bais);
				final Object o = ois.readObject();
				if ((o instanceof SerializableState)
						&& (((SerializableState) o).getObject() instanceof IndexColorModel)) {
					palette = (IndexColorModel) ((SerializableState) o).getObject();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to deserialize color model",
						e);
				palette = null;
			}
		}
		else {
			palette = null;
		}
		if (maxRenderTimeStored) {
			maxRenderTime = buf.getInt();
		}
		else {
			maxRenderTime = 0;
		}
		if (maxErrorsStored) {
			maxErrors = buf.getInt();
		}
		else {
			maxErrors = 0;
		}
		if (angleStored) {
			angle = buf.getDouble();
		}
		else {
			angle = 0;
		}
		if (bufferStored) {
			buffer = buf.getInt();
		}
		else {
			buffer = 0;
		}
		if (bgColorStored) {
			bgColor = new Color(
					buf.getInt());
		}
		else {
			bgColor = null;
		}
		if (styleStored) {
			final byte[] styleBinary = new byte[buf.getInt()];
			buf.get(styleBinary);
			final SLDParser parser = new SLDParser(
					CommonFactoryFinder.getStyleFactory(null),
					new ByteArrayInputStream(
							styleBinary));
			final Style[] styles = parser.readXML();
			if ((styles != null) && (styles.length > 0)) {
				style = styles[0];
			}
			else {
				LOGGER.warn("Unable to deserialize style");
				style = null;
			}
		}
		else {
			style = null;
		}
	}
}
