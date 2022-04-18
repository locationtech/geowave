/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.Sets;

/**
 * Builds up a simplified visibility expression from multiple input visibilities.
 */
public class VisibilityComposer {

  // Hash set would be faster, but tree set makes deterministic visibility expressions
  private final Set<String> visibilityTokens = Sets.newTreeSet();

  /**
   * Constructs an empty visibility composer.
   */
  public VisibilityComposer() {}

  /**
   * Constructs a visibility composer with all of the tokens of another visibility composer.
   * 
   * @param other the starting composer
   */
  public VisibilityComposer(final VisibilityComposer other) {
    visibilityTokens.addAll(other.visibilityTokens);
  }

  /**
   * Add the given visibility expression to the composer. If possible, the expression will be broken
   * down into minimal components.
   *
   * @param visibility the visibility expression to add
   */
  public void addVisibility(final String visibility) {
    if (visibility == null) {
      return;
    }
    VisibilityExpression.addMinimalTokens(visibility, visibilityTokens);
  }

  /**
   * Compose the simplified visibility expression.
   *
   * @return the simplified visibility expression
   */
  public String composeVisibility() {
    return visibilityTokens.stream().collect(Collectors.joining(VisibilityExpression.AND_TOKEN));
  }

}
