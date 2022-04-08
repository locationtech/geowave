/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.cli.converters.GeoWaveBaseConverter;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/** Supports converting the filter string to Filter object. */
public class CQLFilterOptionProvider implements Filter, Persistable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLFilterOptionProvider.class);

  @Parameter(
      names = "--cql",
      description = "A CQL filter, only data matching this filter will be ingested",
      converter = ConvertCQLStrToFilterConverter.class)
  private FilterParameter convertedFilter = new FilterParameter(null, null);

  public CQLFilterOptionProvider() {
    super();
  }

  public String getCqlFilterString() {
    return convertedFilter.getCqlFilterString();
  }

  @Override
  public byte[] toBinary() {
    if (convertedFilter.getCqlFilterString() == null) {
      return new byte[] {};
    }
    return StringUtils.stringToBinary(convertedFilter.getCqlFilterString());
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    if (bytes.length > 0) {
      // This has the side-effect of setting the 'filter' member
      // variable.
      convertedFilter =
          new ConvertCQLStrToFilterConverter().convert(StringUtils.stringFromBinary(bytes));
    } else {
      convertedFilter.setCqlFilterString(null);
      convertedFilter.setFilter(null);
    }
  }

  @Override
  public boolean evaluate(final Object object) {
    if (convertedFilter.getFilter() == null) {
      return true;
    }
    return convertedFilter.getFilter().evaluate(object);
  }

  @Override
  public Object accept(final FilterVisitor visitor, final Object extraData) {
    if (convertedFilter.getFilter() == null) {
      if (visitor != null) {
        return visitor.visitNullFilter(extraData);
      }
      return extraData;
    }
    return convertedFilter.getFilter().accept(visitor, extraData);
  }

  private static Filter asFilter(final String cqlPredicate) throws CQLException {
    return ECQL.toFilter(cqlPredicate);
  }

  /** This class will ensure that as the CQLFilterString is read in and converted to a filter. */
  public static class ConvertCQLStrToFilterConverter extends GeoWaveBaseConverter<FilterParameter> {
    public ConvertCQLStrToFilterConverter() {
      super("");
    }

    public ConvertCQLStrToFilterConverter(final String optionName) {
      super(optionName);
    }

    @Override
    public FilterParameter convert(String value) {
      Filter convertedFilter = null;
      if (value != null) {
        try {
          convertedFilter = asFilter(value);
        }
        // HP Fortify "Log Forging" false positive
        // What Fortify considers "user input" comes only
        // from users with OS-level access anyway
        catch (final CQLException e) {
          LOGGER.error("Cannot parse CQL expression '" + value + "'", e);
          // value = null;
          // convertedFilter = null;
          throw new ParameterException("Cannot parse CQL expression '" + value + "'", e);
        }
      } else {
        value = null;
      }
      return new FilterParameter(value, convertedFilter);
    }
  }

  public static class FilterParameter {
    private String cqlFilterString;
    private Filter filter;

    public FilterParameter(final String cqlFilterString, final Filter filter) {
      super();
      this.cqlFilterString = cqlFilterString;
      this.filter = filter;
    }

    public String getCqlFilterString() {
      return cqlFilterString;
    }

    public void setCqlFilterString(final String cqlFilterString) {
      this.cqlFilterString = cqlFilterString;
    }

    public Filter getFilter() {
      return filter;
    }

    public void setFilter(final Filter filter) {
      this.filter = filter;
    }

    @Override
    public String toString() {
      return cqlFilterString;
    }
  }
}
