/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

/**
 * Abstract class for selecting data from a GeoWave query.
 */
public abstract class Selector {
  private final String alias;
  private final SelectorType type;

  public enum SelectorType {
    AGGREGATION, SIMPLE
  }

  /**
   * @param type the type of this selector
   */
  public Selector(final SelectorType type) {
    this(type, null);
  }

  /**
   * @param type the type of this selector
   * @param alias an alternate display name for the selector
   */
  public Selector(final SelectorType type, final String alias) {
    this.alias = alias;
    this.type = type;
  }

  /**
   * @return the alias of the selector
   */
  public String alias() {
    return alias;
  }

  /**
   * @return the type of this selector
   */
  public SelectorType type() {
    return type;
  }

  /**
   * @return the display name of the selector
   */
  public String name() {
    return alias != null ? alias : selectorName();
  }

  /**
   * @return the non-aliased display name of the selector
   */
  protected abstract String selectorName();
}
