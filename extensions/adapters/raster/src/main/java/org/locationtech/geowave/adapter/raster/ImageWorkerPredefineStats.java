/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster;

import java.awt.RenderingHints;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import javax.media.jai.Histogram;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedImageAdapter;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.image.ImageWorker;

public class ImageWorkerPredefineStats extends ImageWorker {

  public ImageWorkerPredefineStats() {
    super();
    // TODO Auto-generated constructor stub
  }

  public ImageWorkerPredefineStats(final File input) throws IOException {
    super(input);
  }

  public ImageWorkerPredefineStats(final RenderedImage image) {
    super(image);
  }

  public ImageWorkerPredefineStats(final RenderingHints hints) {
    super(hints);
  }

  public ImageWorkerPredefineStats setStats(final Pair<String, Object>[] nameValuePairs) {
    image = new RenderedImageAdapter(image);
    for (final Pair<String, Object> pair : nameValuePairs) {
      ((PlanarImage) (image)).setProperty(pair.getLeft(), pair.getRight());
    }
    return this;
  }

  public ImageWorkerPredefineStats setHistogram(final Histogram histogram) {
    image = new RenderedImageAdapter(image);
    ((PlanarImage) (image)).setProperty("histogram", histogram);
    return this;
  }

  public ImageWorkerPredefineStats setExtrema(final double[][] extrema) {
    image = new RenderedImageAdapter(image);
    ((PlanarImage) (image)).setProperty("extrema", extrema);
    return this;
  }
}
