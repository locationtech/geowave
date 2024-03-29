/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/*
 * JAI-Ext - OpenSource Java Advanced Image Extensions Library http://www.geo-solutions.it/
 * Copyright 2014 GeoSolutions Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.locationtech.geowave.adapter.raster.adapter.warp;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Map;
import javax.media.jai.BorderExtender;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.RasterAccessor;
import javax.media.jai.RasterFormatTag;
import javax.media.jai.Warp;
import javax.media.jai.iterator.RandomIter;
import com.sun.media.jai.util.ImageUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.geosolutions.jaiext.iterators.RandomIterFactory;
import it.geosolutions.jaiext.range.Range;

/**
 * This is code entirely intended to get around an issue on line 265 of WarpOpImage in jai-ext. The
 * following code does not work if the source is significant lower resolution than the destination
 * and seems unnecessary in general:
 *
 * <p> roiTile = roi.intersect(new ROIShape(srcRectExpanded));
 */
@SuppressFBWarnings
public abstract class WarpOpImage extends it.geosolutions.jaiext.warp.WarpOpImage {

  public WarpOpImage(
      final RenderedImage source,
      final ImageLayout layout,
      final Map<?, ?> configuration,
      final boolean cobbleSources,
      final BorderExtender extender,
      final Interpolation interp,
      final Warp warp,
      final double[] backgroundValues,
      final ROI roi,
      final Range noData) {
    super(
        source,
        layout,
        configuration,
        cobbleSources,
        extender,
        interp,
        warp,
        backgroundValues,
        roi,
        noData);
  }

  /**
   * Warps a rectangle. If ROI is present, the intersection between ROI and tile bounds is
   * calculated; The result ROI will be used for calculations inside the computeRect() method.
   */
  @Override
  protected void computeRect(
      final PlanarImage[] sources,
      final WritableRaster dest,
      final Rectangle destRect) {
    // Retrieve format tags.
    final RasterFormatTag[] formatTags = getFormatTags();

    final RasterAccessor dst = new RasterAccessor(dest, destRect, formatTags[1], getColorModel());

    RandomIter roiIter = null;

    boolean roiContainsTile = false;
    boolean roiDisjointTile = false;

    // If a ROI is present, then only the part contained inside the current
    // tile bounds is taken.
    if (hasROI) {
      final Rectangle srcRectExpanded = mapDestRect(destRect, 0);
      // The tile dimension is extended for avoiding border errors
      srcRectExpanded.setRect(
          srcRectExpanded.getMinX() - leftPad,
          srcRectExpanded.getMinY() - topPad,
          srcRectExpanded.getWidth() + rightPad + leftPad,
          srcRectExpanded.getHeight() + bottomPad + topPad);

      if (!roiBounds.intersects(srcRectExpanded)) {
        roiDisjointTile = true;
      } else {
        roiContainsTile = roi.contains(srcRectExpanded);
        if (!roiContainsTile) {
          if (!roi.intersects(srcRectExpanded)) {
            roiDisjointTile = true;
          } else {
            final PlanarImage roiIMG = getImage();
            roiIter = RandomIterFactory.create(roiIMG, null, TILE_CACHED, ARRAY_CALC);
          }
        }
      }
    }

    if (!hasROI || !roiDisjointTile) {
      switch (dst.getDataType()) {
        case DataBuffer.TYPE_BYTE:
          computeRectByte(sources[0], dst, roiIter, roiContainsTile);
          break;
        case DataBuffer.TYPE_USHORT:
          computeRectUShort(sources[0], dst, roiIter, roiContainsTile);
          break;
        case DataBuffer.TYPE_SHORT:
          computeRectShort(sources[0], dst, roiIter, roiContainsTile);
          break;
        case DataBuffer.TYPE_INT:
          computeRectInt(sources[0], dst, roiIter, roiContainsTile);
          break;
        case DataBuffer.TYPE_FLOAT:
          computeRectFloat(sources[0], dst, roiIter, roiContainsTile);
          break;
        case DataBuffer.TYPE_DOUBLE:
          computeRectDouble(sources[0], dst, roiIter, roiContainsTile);
          break;
      }
      // After the calculations, the output data are copied into the
      // WritableRaster
      if (dst.isDataCopy()) {
        dst.clampDataArrays();
        dst.copyDataToRaster();
      }
    } else {
      // If the tile is outside the ROI, then the destination Raster is
      // set to backgroundValues
      if (setBackground) {
        ImageUtil.fillBackground(dest, destRect, backgroundValues);
      }
    }
  }

  /**
   * This method provides a lazy initialization of the image associated to the ROI. The method uses
   * the Double-checked locking in order to maintain thread-safety
   *
   * @return
   */
  private PlanarImage getImage() {
    PlanarImage img = roiImage;
    // HP Fortify "Double-Checked Locking" false positive
    // This is not a security issue. We are aware of the extremely small
    // potential for this to be called twice, but that is not an
    // inconsistency and is more than worth the performance gains
    if (img == null) {
      synchronized (this) {
        img = roiImage;
        if (img == null) {
          roiImage = img = roi.getAsImage();
        }
      }
    }
    return img;
  }
}
