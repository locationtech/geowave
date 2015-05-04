/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2004-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package mil.nga.giat.geowave.adapter.vector.wms;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.awt.Color;
import java.awt.Composite;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.Image;
import java.awt.Paint;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.Transparency;
import java.awt.font.FontRenderContext;
import java.awt.font.GlyphVector;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ColorModel;
import java.awt.image.ImageObserver;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.RenderableImage;
import java.text.AttributedCharacterIterator;
import java.util.Map;

/**
 * A graphic drawing on a BufferedImage compatible with a main graphic. Used to
 * delay the allocation of the back buffer until the last moment
 * 
 * This class originated from GeoTools, but was final and required some
 * modifications (the raster callback in particular)
 * 
 * @author Andrea Aime - OpenGeo (author of the original, it has been modified)
 */
public final class DelayedBackbufferGraphic extends
		Graphics2D
{
	Graphics2D master;

	WritableRasterCallback rasterCallback;

	Rectangle screenSize;

	public DelayedBackbufferGraphic(
			final Graphics2D master,
			final Rectangle screenSize ) {
		this(
				master,
				screenSize,
				null);
	}

	public DelayedBackbufferGraphic(
			final Graphics2D master,
			final Rectangle screenSize,
			final WritableRasterCallback rasterCallback ) {
		this.master = master;
		this.screenSize = screenSize;
		this.rasterCallback = rasterCallback;
	}

	BufferedImage image;

	Graphics2D delegate;

	public synchronized void setCallback(
			final WritableRasterCallback rasterCallback ) {
		this.rasterCallback = rasterCallback;

		if (delegate != null) {
			final ColorModel cm = image.getColorModel();
			final boolean isAlphaPremultiplied = cm.isAlphaPremultiplied();
			final WritableRaster raster = new EventingWritableRaster(
					image.getRaster(),
					this.rasterCallback);
			image = new BufferedImage(
					cm,
					raster,
					isAlphaPremultiplied,
					null);
			this.rasterCallback.setImage(image);
			delegate = image.createGraphics();
			delegate.setRenderingHints(master.getRenderingHints());
		}
	}

	/**
	 * Call this method before starting to use the graphic for good
	 */
	public synchronized void init() {
		if (delegate == null) {
			image = master.getDeviceConfiguration().createCompatibleImage(
					screenSize.width,
					screenSize.height,
					Transparency.TRANSLUCENT);
			if (rasterCallback != null) {
				final ColorModel cm = image.getColorModel();
				final boolean isAlphaPremultiplied = cm.isAlphaPremultiplied();
				final WritableRaster raster = new EventingWritableRaster(
						image.getRaster(),
						rasterCallback);
				image = new BufferedImage(
						cm,
						raster,
						isAlphaPremultiplied,
						null);

				rasterCallback.setImage(image);
			}
			delegate = image.createGraphics();
			delegate.setRenderingHints(master.getRenderingHints());
		}
	}

	public synchronized BufferedImage getImage() {
		return image;
	}

	@Override
	public void addRenderingHints(
			final Map<?, ?> hints ) {
		delegate.addRenderingHints(hints);
	}

	@Override
	public void clearRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.clearRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void clip(
			final Shape s ) {
		delegate.clip(s);
	}

	@Override
	public void clipRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.clipRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void copyArea(
			final int x,
			final int y,
			final int width,
			final int height,
			final int dx,
			final int dy ) {
		delegate.copyArea(
				x,
				y,
				width,
				height,
				dx,
				dy);
	}

	@Override
	public Graphics create() {
		return delegate.create();
	}

	@Override
	public Graphics create(
			final int x,
			final int y,
			final int width,
			final int height ) {
		return delegate.create(
				x,
				y,
				width,
				height);
	}

	@Override
	public void dispose() {
		if (delegate != null) {
			delegate.dispose();
		}
	}

	@Override
	public void draw(
			final Shape s ) {
		delegate.draw(s);
	}

	@Override
	public void draw3DRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final boolean raised ) {
		delegate.draw3DRect(
				x,
				y,
				width,
				height,
				raised);
	}

	@Override
	public void drawArc(
			final int x,
			final int y,
			final int width,
			final int height,
			final int startAngle,
			final int arcAngle ) {
		delegate.drawArc(
				x,
				y,
				width,
				height,
				startAngle,
				arcAngle);
	}

	@Override
	public void drawBytes(
			final byte[] data,
			final int offset,
			final int length,
			final int x,
			final int y ) {
		delegate.drawBytes(
				data,
				offset,
				length,
				x,
				y);
	}

	@Override
	public void drawChars(
			final char[] data,
			final int offset,
			final int length,
			final int x,
			final int y ) {
		delegate.drawChars(
				data,
				offset,
				length,
				x,
				y);
	}

	@Override
	public void drawGlyphVector(
			final GlyphVector g,
			final float x,
			final float y ) {
		delegate.drawGlyphVector(
				g,
				x,
				y);
	}

	@Override
	public void drawImage(
			final BufferedImage img,
			final BufferedImageOp op,
			final int x,
			final int y ) {
		delegate.drawImage(
				img,
				op,
				x,
				y);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final AffineTransform xform,
			final ImageObserver obs ) {
		return delegate.drawImage(
				img,
				xform,
				obs);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				x,
				y,
				bgcolor,
				observer);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				x,
				y,
				observer);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final int width,
			final int height,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				x,
				y,
				width,
				height,
				bgcolor,
				observer);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final int width,
			final int height,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				x,
				y,
				width,
				height,
				observer);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int dx1,
			final int dy1,
			final int dx2,
			final int dy2,
			final int sx1,
			final int sy1,
			final int sx2,
			final int sy2,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				dx1,
				dy1,
				dx2,
				dy2,
				sx1,
				sy1,
				sx2,
				sy2,
				bgcolor,
				observer);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int dx1,
			final int dy1,
			final int dx2,
			final int dy2,
			final int sx1,
			final int sy1,
			final int sx2,
			final int sy2,
			final ImageObserver observer ) {
		return delegate.drawImage(
				img,
				dx1,
				dy1,
				dx2,
				dy2,
				sx1,
				sy1,
				sx2,
				sy2,
				observer);
	}

	@Override
	public void drawLine(
			final int x1,
			final int y1,
			final int x2,
			final int y2 ) {
		delegate.drawLine(
				x1,
				y1,
				x2,
				y2);
	}

	@Override
	public void drawOval(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.drawOval(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawPolygon(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delegate.drawPolygon(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void drawPolygon(
			final Polygon p ) {
		delegate.drawPolygon(p);
	}

	@Override
	public void drawPolyline(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delegate.drawPolyline(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void drawRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.drawRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawRenderableImage(
			final RenderableImage img,
			final AffineTransform xform ) {
		delegate.drawRenderableImage(
				img,
				xform);
	}

	@Override
	public void drawRenderedImage(
			final RenderedImage img,
			final AffineTransform xform ) {
		delegate.drawRenderedImage(
				img,
				xform);
	}

	@Override
	public void drawRoundRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final int arcWidth,
			final int arcHeight ) {
		delegate.drawRoundRect(
				x,
				y,
				width,
				height,
				arcWidth,
				arcHeight);
	}

	@Override
	public void drawString(
			final AttributedCharacterIterator iterator,
			final float x,
			final float y ) {
		delegate.drawString(
				iterator,
				x,
				y);
	}

	@Override
	public void drawString(
			final AttributedCharacterIterator iterator,
			final int x,
			final int y ) {
		delegate.drawString(
				iterator,
				x,
				y);
	}

	@Override
	public void drawString(
			final String s,
			final float x,
			final float y ) {
		delegate.drawString(
				s,
				x,
				y);
	}

	@Override
	public void drawString(
			final String str,
			final int x,
			final int y ) {
		delegate.drawString(
				str,
				x,
				y);
	}

	@Override
	public void fill(
			final Shape s ) {
		delegate.fill(s);
	}

	@Override
	public void fill3DRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final boolean raised ) {
		delegate.fill3DRect(
				x,
				y,
				width,
				height,
				raised);
	}

	@Override
	public void fillArc(
			final int x,
			final int y,
			final int width,
			final int height,
			final int startAngle,
			final int arcAngle ) {
		delegate.fillArc(
				x,
				y,
				width,
				height,
				startAngle,
				arcAngle);
	}

	@Override
	public void fillOval(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.fillOval(
				x,
				y,
				width,
				height);
	}

	@Override
	public void fillPolygon(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delegate.fillPolygon(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void fillPolygon(
			final Polygon p ) {
		delegate.fillPolygon(p);
	}

	@Override
	public void fillRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.fillRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void fillRoundRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final int arcWidth,
			final int arcHeight ) {
		delegate.fillRoundRect(
				x,
				y,
				width,
				height,
				arcWidth,
				arcHeight);
	}

	@SuppressFBWarnings(value = {
		"FI_PUBLIC_SHOULD_BE_PROTECTED",
		"FI_EXPLICIT_INVOCATION"
	}, justification = "Access defined in java.awt.Graphics; finalization only called by container resource finalize method)_")
	@Override
	public void finalize() {
		super.finalize();
		delegate.finalize();
	}

	@Override
	public Color getBackground() {
		return delegate.getBackground();
	}

	@Override
	public Shape getClip() {
		return delegate.getClip();
	}

	@Override
	public Rectangle getClipBounds() {
		return delegate.getClipBounds();
	}

	@Override
	public Rectangle getClipBounds(
			final Rectangle r ) {
		return delegate.getClipBounds(r);
	}

	@Override
	public Rectangle getClipRect() {
		return delegate.getClipRect();
	}

	@Override
	public Color getColor() {
		return delegate.getColor();
	}

	@Override
	public Composite getComposite() {
		return delegate.getComposite();
	}

	@Override
	public GraphicsConfiguration getDeviceConfiguration() {
		return delegate.getDeviceConfiguration();
	}

	@Override
	public Font getFont() {
		return delegate.getFont();
	}

	@Override
	public FontMetrics getFontMetrics() {
		return delegate.getFontMetrics();
	}

	@Override
	public FontMetrics getFontMetrics(
			final Font f ) {
		return delegate.getFontMetrics(f);
	}

	@Override
	public FontRenderContext getFontRenderContext() {
		return delegate.getFontRenderContext();
	}

	@Override
	public Paint getPaint() {
		return delegate.getPaint();
	}

	@Override
	public Object getRenderingHint(
			final Key hintKey ) {
		return delegate.getRenderingHint(hintKey);
	}

	@Override
	public RenderingHints getRenderingHints() {
		return delegate.getRenderingHints();
	}

	@Override
	public Stroke getStroke() {
		return delegate.getStroke();
	}

	@Override
	public AffineTransform getTransform() {
		return delegate.getTransform();
	}

	@Override
	public boolean hit(
			final Rectangle rect,
			final Shape s,
			final boolean onStroke ) {
		return delegate.hit(
				rect,
				s,
				onStroke);
	}

	@Override
	public boolean hitClip(
			final int x,
			final int y,
			final int width,
			final int height ) {
		return delegate.hitClip(
				x,
				y,
				width,
				height);
	}

	@Override
	public void rotate(
			final double theta,
			final double x,
			final double y ) {
		delegate.rotate(
				theta,
				x,
				y);
	}

	@Override
	public void rotate(
			final double theta ) {
		delegate.rotate(theta);
	}

	@Override
	public void scale(
			final double sx,
			final double sy ) {
		delegate.scale(
				sx,
				sy);
	}

	@Override
	public void setBackground(
			final Color color ) {
		delegate.setBackground(color);
	}

	@Override
	public void setClip(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delegate.setClip(
				x,
				y,
				width,
				height);
	}

	@Override
	public void setClip(
			final Shape clip ) {
		delegate.setClip(clip);
	}

	@Override
	public void setColor(
			final Color c ) {
		delegate.setColor(c);
	}

	@Override
	public void setComposite(
			final Composite comp ) {
		delegate.setComposite(comp);
	}

	@Override
	public void setFont(
			final Font font ) {
		delegate.setFont(font);
	}

	@Override
	public void setPaint(
			final Paint paint ) {
		delegate.setPaint(paint);
	}

	@Override
	public void setPaintMode() {
		delegate.setPaintMode();
	}

	@Override
	public void setRenderingHint(
			final Key hintKey,
			final Object hintValue ) {
		delegate.setRenderingHint(
				hintKey,
				hintValue);
	}

	@Override
	public void setRenderingHints(
			final Map<?, ?> hints ) {
		delegate.setRenderingHints(hints);
	}

	@Override
	public void setStroke(
			final Stroke s ) {
		delegate.setStroke(s);
	}

	@Override
	public void setTransform(
			final AffineTransform Tx ) {
		delegate.setTransform(Tx);
	}

	@Override
	public void setXORMode(
			final Color c1 ) {
		delegate.setXORMode(c1);
	}

	@Override
	public void shear(
			final double shx,
			final double shy ) {
		delegate.shear(
				shx,
				shy);
	}

	@Override
	public void transform(
			final AffineTransform Tx ) {
		delegate.transform(Tx);
	}

	@Override
	public void translate(
			final double tx,
			final double ty ) {
		delegate.translate(
				tx,
				ty);
	}

	@Override
	public void translate(
			final int x,
			final int y ) {
		delegate.translate(
				x,
				y);
	}

}
