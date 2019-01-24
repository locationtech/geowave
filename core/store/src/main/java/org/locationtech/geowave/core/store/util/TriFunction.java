/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface TriFunction<A, B, C, R> {

  R apply(A a, B b, C c);

  default <V> TriFunction<A, B, C, V> andThen(final Function<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return (final A a, final B b, final C c) -> after.apply(apply(a, b, c));
  }
}
