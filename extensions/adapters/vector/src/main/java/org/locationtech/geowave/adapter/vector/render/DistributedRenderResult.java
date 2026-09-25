/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.render;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.geoserver.wms.map.ImageUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;

public class DistributedRenderResult implements Mergeable {
  public static class CompositeGroupResult implements Mergeable {
    private PersistableComposite composite;

    // keep each style separate so they can be composited together in the
    // original draw order
    private List<Pair<PersistableRenderedImage, PersistableComposite>> orderedStyles;

    public CompositeGroupResult() {}

    public CompositeGroupResult(
        final PersistableComposite composite,
        final List<Pair<PersistableRenderedImage, PersistableComposite>> orderedStyles) {
      this.composite = composite;
      this.orderedStyles = orderedStyles;
    }

    private void render(final Graphics2D parentGraphics, final int width, final int height) {
      Graphics2D graphics;
      BufferedImage compositeGroupImage = null;
      if ((composite != null) && (composite.getComposite() != null)) {
        // this will render to a back buffer so that

        compositeGroupImage =
            parentGraphics.getDeviceConfiguration().createCompatibleImage(
                width,
                height,
                Transparency.TRANSLUCENT);
        graphics = compositeGroupImage.createGraphics();
        graphics.setRenderingHints(parentGraphics.getRenderingHints());
      } else {
        graphics = parentGraphics;
      }
      for (final Pair<PersistableRenderedImage, PersistableComposite> currentStyle : orderedStyles) {
        if ((currentStyle == null)
            || (currentStyle.getKey() == null)
            || (currentStyle.getKey().image == null)) {
          continue;
        }
        if ((currentStyle.getValue() == null) || (currentStyle.getValue().getComposite() == null)) {
          graphics.setComposite(AlphaComposite.SrcOver);
        } else {
          graphics.setComposite(currentStyle.getValue().getComposite());
        }
        graphics.drawImage(currentStyle.getKey().image, 0, 0, null);
      }
      if (compositeGroupImage != null) {
        if ((composite == null) || (composite.getComposite() == null)) {
          parentGraphics.setComposite(AlphaComposite.SrcOver);
        } else {
          parentGraphics.setComposite(composite.getComposite());
        }
        parentGraphics.drawImage(compositeGroupImage, 0, 0, null);
        graphics.dispose();
      }
    }

    @Override
    public byte[] toBinary() {
      final byte[] compositeBinary;
      if (composite != null) {
        compositeBinary = composite.toBinary();
      } else {
        compositeBinary = new byte[] {};
      }
      final List<byte[]> styleBinaries = new ArrayList<>(orderedStyles.size());
      int bufferSize =
          compositeBinary.length + VarintUtils.unsignedIntByteLength(compositeBinary.length);
      for (final Pair<PersistableRenderedImage, PersistableComposite> style : orderedStyles) {
        byte[] styleBinary;
        if (style != null) {
          byte[] styleCompositeBinary;
          if (style.getRight() != null) {
            styleCompositeBinary = style.getRight().toBinary();
          } else {
            styleCompositeBinary = new byte[] {};
          }
          byte[] styleImageBinary;
          if (style.getLeft() != null) {
            styleImageBinary = style.getLeft().toBinary();
          } else {
            styleImageBinary = new byte[] {};
          }
          final ByteBuffer styleBuf =
              ByteBuffer.allocate(
                  styleCompositeBinary.length
                      + styleImageBinary.length
                      + VarintUtils.unsignedIntByteLength(styleCompositeBinary.length));
          VarintUtils.writeUnsignedInt(styleCompositeBinary.length, styleBuf);
          if (styleCompositeBinary.length > 0) {
            styleBuf.put(styleCompositeBinary);
          }
          if (styleImageBinary.length > 0) {
            styleBuf.put(styleImageBinary);
          }

          styleBinary = styleBuf.array();
        } else {
          styleBinary = new byte[] {};
        }

        styleBinaries.add(styleBinary);
        bufferSize += (styleBinary.length + VarintUtils.unsignedIntByteLength(styleBinary.length));
      }
      bufferSize += VarintUtils.unsignedIntByteLength(styleBinaries.size());
      final ByteBuffer buf = ByteBuffer.allocate(bufferSize);
      VarintUtils.writeUnsignedInt(compositeBinary.length, buf);
      if (compositeBinary.length > 0) {
        buf.put(compositeBinary);
      }
      VarintUtils.writeUnsignedInt(styleBinaries.size(), buf);
      for (final byte[] styleBinary : styleBinaries) {
        VarintUtils.writeUnsignedInt(styleBinary.length, buf);
        buf.put(styleBinary);
      }
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int compositeBinaryLength = VarintUtils.readUnsignedInt(buf);
      if (compositeBinaryLength > 0) {
        final byte[] compositeBinary = ByteArrayUtils.safeRead(buf, compositeBinaryLength);
        composite = new PersistableComposite();
        composite.fromBinary(compositeBinary);
      } else {
        composite = null;
      }
      final int styleLength = VarintUtils.readUnsignedInt(buf);
      ByteArrayUtils.verifyBufferSize(buf, styleLength);
      orderedStyles = new ArrayList<>(styleLength);
      for (int i = 0; i < styleLength; i++) {

        final int styleBinaryLength = VarintUtils.readUnsignedInt(buf);
        if (styleBinaryLength > 0) {
          final byte[] styleBinary = ByteArrayUtils.safeRead(buf, styleBinaryLength);
          final ByteBuffer styleBuf = ByteBuffer.wrap(styleBinary);
          final int styleCompositeBinaryLength = VarintUtils.readUnsignedInt(styleBuf);
          PersistableComposite styleComposite;
          if (styleCompositeBinaryLength > 0) {
            final byte[] styleCompositeBinary =
                ByteArrayUtils.safeRead(styleBuf, styleCompositeBinaryLength);
            styleComposite = new PersistableComposite();
            styleComposite.fromBinary(styleCompositeBinary);
          } else {
            styleComposite = null;
          }
          final int styleImageBinaryLength = styleBuf.remaining();
          PersistableRenderedImage styleImage;
          if (styleImageBinaryLength > 0) {
            final byte[] styleImageBinary = new byte[styleImageBinaryLength];
            styleBuf.get(styleImageBinary);
            styleImage = new PersistableRenderedImage();
            styleImage.fromBinary(styleImageBinary);
          } else {
            styleImage = null;
          }
          orderedStyles.add(Pair.of(styleImage, styleComposite));
        } else {
          orderedStyles.add(null);
        }
      }
    }

    @Override
    public void merge(final Mergeable merge) {
      if (merge instanceof CompositeGroupResult) {
        final CompositeGroupResult other = (CompositeGroupResult) merge;

        final List<Pair<PersistableRenderedImage, PersistableComposite>> newOrderedStyles =
            new ArrayList<>();
        final int minStyles = Math.min(orderedStyles.size(), other.orderedStyles.size());
        for (int i = 0; i < minStyles; i++) {
          final Pair<PersistableRenderedImage, PersistableComposite> thisStyle =
              orderedStyles.get(i);
          final Pair<PersistableRenderedImage, PersistableComposite> otherStyle =
              other.orderedStyles.get(i);
          // all composites should be the same, if they're not then
          // these composite groups got mis-ordered by style

          // keep in mind that they can be null if nothing was
          // rendered to this style or other style because of rules
          // applied to that specific subset of data not resulting in
          // anything rendered for the style
          if (thisStyle != null) {
            if (otherStyle != null) {
              // render the images together and just arbitrarily
              // grab "this" composite as they both should be the
              // same
              newOrderedStyles.add(
                  Pair.of(
                      mergeImage(thisStyle.getLeft(), otherStyle.getLeft()),
                      thisStyle.getRight()));
            } else {
              newOrderedStyles.add(thisStyle);
            }
          } else {
            newOrderedStyles.add(otherStyle);
          }
        }

        if (orderedStyles.size() > minStyles) {
          // hopefully this is never the case, but just in case
          newOrderedStyles.addAll(orderedStyles.subList(minStyles, orderedStyles.size()));
        }
        if (other.orderedStyles.size() > minStyles) {
          // hopefully this is never the case, but just in case
          newOrderedStyles.addAll(
              other.orderedStyles.subList(minStyles, other.orderedStyles.size()));
        }
        orderedStyles = newOrderedStyles;
      }
    }
  }

  // geotools has a concept of composites, which we need to keep separate so
  // that they can be composited in the original draw order, by default there
  // is only a single composite
  private List<CompositeGroupResult> orderedComposites;
  // the parent image essentially gets labels rendered to it
  private PersistableRenderedImage parentImage;

  public DistributedRenderResult() {}

  public DistributedRenderResult(
      final PersistableRenderedImage parentImage,
      final List<CompositeGroupResult> orderedComposites) {
    this.parentImage = parentImage;
    this.orderedComposites = orderedComposites;
  }

  public BufferedImage renderComposite(final DistributedRenderOptions renderOptions) {
    final BufferedImage image =
        ImageUtils.createImage(
            renderOptions.getMapWidth(),
            renderOptions.getMapHeight(),
            renderOptions.getPalette(),
            renderOptions.isTransparent() || renderOptions.isMetatile());
    final Graphics2D graphics =
        ImageUtils.prepareTransparency(
            renderOptions.isTransparent(),
            renderOptions.getBgColor(),
            image,
            null);
    for (final CompositeGroupResult compositeGroup : orderedComposites) {
      compositeGroup.render(graphics, renderOptions.getMapWidth(), renderOptions.getMapHeight());
    }
    final BufferedImage img = parentImage.getImage();
    graphics.drawImage(img, 0, 0, null);
    graphics.dispose();
    return image;
  }

  @Override
  public byte[] toBinary() {
    // 4 bytes for the length as an int, and 4 bytes for the size of
    // parentImage
    final byte[] parentImageBinary = parentImage.toBinary();
    int byteSize =
        VarintUtils.unsignedIntByteLength(parentImageBinary.length)
            + parentImageBinary.length
            + VarintUtils.unsignedIntByteLength(orderedComposites.size());
    final List<byte[]> compositeBinaries = new ArrayList<>(orderedComposites.size());
    for (final CompositeGroupResult compositeGroup : orderedComposites) {
      final byte[] compositeGroupBinary = compositeGroup.toBinary();
      byteSize +=
          (compositeGroupBinary.length
              + VarintUtils.unsignedIntByteLength(compositeGroupBinary.length));
      compositeBinaries.add(compositeGroupBinary);
    }
    final ByteBuffer buf = ByteBuffer.allocate(byteSize);
    VarintUtils.writeUnsignedInt(parentImageBinary.length, buf);
    buf.put(parentImageBinary);
    VarintUtils.writeUnsignedInt(orderedComposites.size(), buf);
    for (final byte[] compositeGroupBinary : compositeBinaries) {
      VarintUtils.writeUnsignedInt(compositeGroupBinary.length, buf);
      buf.put(compositeGroupBinary);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] parentImageBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    parentImage = new PersistableRenderedImage();
    parentImage.fromBinary(parentImageBinary);
    final int numCompositeGroups = VarintUtils.readUnsignedInt(buf);
    orderedComposites = new ArrayList<>(numCompositeGroups);
    for (int i = 0; i < numCompositeGroups; i++) {
      final byte[] compositeGroupBinary =
          ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      final CompositeGroupResult compositeGroup = new CompositeGroupResult();
      compositeGroup.fromBinary(compositeGroupBinary);
      orderedComposites.add(compositeGroup);
    }
  }

  @Override
  public void merge(final Mergeable merge) {
    if (merge instanceof DistributedRenderResult) {
      final DistributedRenderResult other = ((DistributedRenderResult) merge);
      final int minComposites = Math.min(orderedComposites.size(), other.orderedComposites.size());
      // first render parents together
      if ((parentImage != null) && (parentImage.image != null)) {
        if ((other.parentImage != null) && (other.parentImage.image != null)) {
          // all composites should be the same, if they're not
          // then these distributed results got mis-ordered by
          // composite group, so composite remains this.composite
          parentImage = mergeImage(parentImage, other.parentImage);
        }
      } else {
        parentImage = other.parentImage;
      }
      final List<CompositeGroupResult> newOrderedComposites = new ArrayList<>();
      for (int c = 0; c < minComposites; c++) {
        final CompositeGroupResult thisCompositeGroup = orderedComposites.get(c);
        final CompositeGroupResult otherCompositeGroup = other.orderedComposites.get(c);
        thisCompositeGroup.merge(otherCompositeGroup);
        newOrderedComposites.add(thisCompositeGroup);
      }
      if (orderedComposites.size() > minComposites) {
        // hopefully this is never the case, but just in case
        newOrderedComposites.addAll(
            orderedComposites.subList(minComposites, orderedComposites.size()));
      }
      if (other.orderedComposites.size() > minComposites) {
        // hopefully this is never the case, but just in case
        newOrderedComposites.addAll(
            other.orderedComposites.subList(minComposites, other.orderedComposites.size()));
      }
      orderedComposites = newOrderedComposites;
    }
  }

  private static PersistableRenderedImage mergeImage(
      final PersistableRenderedImage image1,
      final PersistableRenderedImage image2) {
    final Graphics2D graphics = image1.image.createGraphics();
    graphics.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER));
    graphics.drawImage(image2.image, 0, 0, null);
    graphics.dispose();
    return new PersistableRenderedImage(image1.image);
  }
}
