/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

/**
 * The medium an entity moves in. Each domain has its own stored H3 levels ({@link ResolutionPlan}),
 * gap and plausibility limits ({@link GapPolicy}) and vector defaults ({@link MotionVectorLayout}),
 * and is expected to be written to its own data type.
 */
public enum MotionDomain {
  SURFACE, AIR, GROUND, UNKNOWN
}
