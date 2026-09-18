/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.parsed;

/**
 * Anchors the package that JCommander facade classes are generated into.
 *
 * <p> Javassist defines a generated class through a {@code MethodHandles.Lookup}, which must come
 * from a class already in the target package. Without a real class here there is nothing to acquire
 * that lookup from, and Javassist falls back to reflecting into {@code ClassLoader.defineClass},
 * which throws {@code InaccessibleObjectException} on JDK 9 and above.
 */
public final class GeneratedClassAnchor {
  private GeneratedClassAnchor() {}
}
