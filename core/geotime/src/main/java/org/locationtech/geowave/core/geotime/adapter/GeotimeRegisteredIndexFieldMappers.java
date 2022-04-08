/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import org.locationtech.geowave.core.geotime.adapter.LatLonFieldMapper.DoubleLatLonFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.LatLonFieldMapper.FloatLatLonFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TemporalLongFieldMapper.CalendarLongFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TemporalLongFieldMapper.DateLongFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeInstantFieldMapper.CalendarInstantFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeInstantFieldMapper.DateInstantFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeInstantFieldMapper.LongInstantFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeRangeFieldMapper.CalendarRangeFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeRangeFieldMapper.DateRangeFieldMapper;
import org.locationtech.geowave.core.geotime.adapter.TimeRangeFieldMapper.LongRangeFieldMapper;
import org.locationtech.geowave.core.store.index.IndexFieldMapperRegistrySPI;

/**
 * Registered spatial and temporal adapter to index field mappers.
 */
public class GeotimeRegisteredIndexFieldMappers implements IndexFieldMapperRegistrySPI {

  @Override
  public RegisteredFieldMapper[] getRegisteredFieldMappers() {
    return new RegisteredFieldMapper[] {
        new RegisteredFieldMapper(DateLongFieldMapper::new, (short) 306),
        new RegisteredFieldMapper(CalendarLongFieldMapper::new, (short) 307),
        new RegisteredFieldMapper(GeometryFieldMapper::new, (short) 350),
        new RegisteredFieldMapper(DoubleLatLonFieldMapper::new, (short) 351),
        new RegisteredFieldMapper(FloatLatLonFieldMapper::new, (short) 352),
        new RegisteredFieldMapper(CalendarInstantFieldMapper::new, (short) 353),
        new RegisteredFieldMapper(DateInstantFieldMapper::new, (short) 354),
        new RegisteredFieldMapper(LongInstantFieldMapper::new, (short) 355),
        new RegisteredFieldMapper(CalendarRangeFieldMapper::new, (short) 356),
        new RegisteredFieldMapper(DateRangeFieldMapper::new, (short) 357),
        new RegisteredFieldMapper(LongRangeFieldMapper::new, (short) 358),};
  }

}
