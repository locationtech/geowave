/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.Arrays;

/**
 * The H3 levels stored per domain and the levels each source may contribute to.
 *
 * <p> Default stored levels (analysis, overview): SURFACE 6 and 4, AIR 5 and 3, GROUND 7 and 5,
 * UNKNOWN 6 and 4. A source contributes only at levels whose average hexagon edge is at least
 * {@code capFactor x (tolerance + sigma)} (default factor 3), or at its explicit
 * {@link MotionSource#withMaxLevel(int) max level} and coarser. For example a source with a 300 m
 * tolerance and a 1 km sigma is capped at 5, so with the SURFACE defaults it contributes at level 4
 * only.
 */
public final class ResolutionPlan {
  private static final int DOMAINS = MotionDomain.values().length;

  private final int[][] levels;
  private final double capFactor;

  private ResolutionPlan(final Builder builder) {
    levels = new int[DOMAINS][];
    for (int i = 0; i < DOMAINS; i++) {
      levels[i] = builder.levels[i].clone();
    }
    capFactor = builder.capFactor;
  }

  public static ResolutionPlan defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** The stored levels of {@code domain}, finest first. */
  public int[] getLevels(final MotionDomain domain) {
    return levels[domain.ordinal()].clone();
  }

  public double getCapFactor() {
    return capFactor;
  }

  /** The finest level {@code source} may contribute to. */
  public int getMaxLevel(final MotionSource source) {
    if (source.hasMaxLevel()) {
      return source.getMaxLevel();
    }
    final double minEdge =
        capFactor * (source.getToleranceMeters() + source.getPositionSigmaMeters());
    int level = 0;
    while ((level < 15) && (H3Holder.averageEdgeMeters(level + 1) >= minEdge)) {
      level++;
    }
    return level;
  }

  /**
   * The stored levels of {@code domain} that {@code source} contributes to, finest first. Empty
   * when the source is too coarse for every stored level.
   */
  public int[] getContributingLevels(final MotionDomain domain, final MotionSource source) {
    final int maxLevel = getMaxLevel(source);
    return Arrays.stream(levels[domain.ordinal()]).filter(l -> l <= maxLevel).toArray();
  }

  public static final class Builder {
    private final int[][] levels = new int[DOMAINS][];
    private double capFactor = 3.0;

    private Builder() {
      levels(MotionDomain.SURFACE, 6, 4);
      levels(MotionDomain.AIR, 5, 3);
      levels(MotionDomain.GROUND, 7, 5);
      levels(MotionDomain.UNKNOWN, 6, 4);
    }

    /** Sets the stored levels of {@code domain}, in any order. */
    public Builder levels(final MotionDomain domain, final int... domainLevels) {
      final int[] sorted =
          Arrays.stream(domainLevels).distinct().boxed().sorted((a, b) -> b - a).mapToInt(
              Integer::intValue).toArray();
      if ((sorted.length == 0) || (sorted[0] > 15) || (sorted[sorted.length - 1] < 0)) {
        throw new IllegalArgumentException("Levels must be one or more H3 resolutions (0-15)");
      }
      levels[domain.ordinal()] = sorted;
      return this;
    }

    public Builder capFactor(final double factor) {
      if (!(factor >= 0)) {
        throw new IllegalArgumentException("The cap factor must be non-negative");
      }
      capFactor = factor;
      return this;
    }

    public ResolutionPlan build() {
      return new ResolutionPlan(this);
    }
  }

  @Override
  public String toString() {
    return "ResolutionPlan [levels="
        + Arrays.deepToString(levels)
        + ", capFactor="
        + capFactor
        + "]";
  }
}
