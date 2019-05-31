package org.locationtech.geowave.core.geotime.index.dimension;

import org.locationtech.geowave.core.index.simple.SimpleLongIndexStrategy;

public class SimpleTimeIndexStrategy extends SimpleLongIndexStrategy {


  public SimpleTimeIndexStrategy() {
    super(new SimpleTimeDefinition());
  }

}
