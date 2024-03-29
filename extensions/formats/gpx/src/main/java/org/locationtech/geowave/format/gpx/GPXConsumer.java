/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.gpx;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes a GPX file. The consumer is an iterator, parsing the input stream and returning results
 * as the stream is parsed. Data is emitted for each element at the 'end' tag.
 *
 * <p> Caution: Developers should maintain the cohesiveness of attribute names associated with each
 * feature type defined in {@link GpxUtils}.
 *
 * <p> Route way points and way points are treated similarly except way points do not include the
 * parent ID information in their ID. The assumption is that the name, lat and lon attributes are
 * globally unique. In contrast, Route way points include the file name and parent route name as
 * part of their ID. Routes are not assumed to be global.
 */
public class GPXConsumer implements CloseableIterator<GeoWaveData<SimpleFeature>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GpxIngestPlugin.class);

  private final SimpleFeatureBuilder pointBuilder;
  private final SimpleFeatureBuilder waypointBuilder;
  private final SimpleFeatureBuilder routeBuilder;
  private final SimpleFeatureBuilder trackBuilder;
  protected static final SimpleFeatureType pointType = GpxUtils.createGPXPointDataType();
  protected static final SimpleFeatureType waypointType = GpxUtils.createGPXWaypointDataType();

  protected static final SimpleFeatureType trackType = GpxUtils.createGPXTrackDataType();
  protected static final SimpleFeatureType routeType = GpxUtils.createGPXRouteDataType();

  final InputStream fileStream;
  final String[] indexNames;
  final String inputID;
  final Map<String, Map<String, String>> additionalData;
  final boolean uniqueWayPoints;
  final double maxLength;

  final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

  final Stack<GPXDataElement> currentElementStack = new Stack<>();
  GPXDataElement top = null;

  static final NumberFormat LatLongFormat = new DecimalFormat("0000000000");

  XMLEventReader eventReader;
  GeoWaveData<SimpleFeature> nextFeature = null;
  private final Long backupTimestamp;

  /**
   * @param fileStream
   * @param indexNames
   * @param inputID prefix to all IDs except waypoints (see uniqueWayPoints)
   * @param additionalData additional attributes to add the over-ride attributes in the GPX data
   *        file. The attribute are grouped by path. "gpx.trk", "gpx.rte" and "gpx.wpt"
   * @param backTimestamp
   * @param uniqueWayPoints if true, waypoints are globally unique, otherwise are unique to this
   *        file and should have inputID and other components added to the identifier
   * @param globalVisibility
   * @param maxLength
   */
  public GPXConsumer(
      final InputStream fileStream,
      final String[] indexNames,
      final String inputID,
      final Map<String, Map<String, String>> additionalData,
      final Long backTimestamp,
      final boolean uniqueWayPoints,
      final double maxLength) {
    super();
    this.fileStream = fileStream;
    this.indexNames = indexNames;
    this.inputID = inputID != null ? inputID : "";
    this.uniqueWayPoints = uniqueWayPoints;
    this.additionalData = additionalData;
    this.maxLength = maxLength;
    backupTimestamp = backTimestamp;
    top = new GPXDataElement("gpx", this.maxLength);
    pointBuilder = new SimpleFeatureBuilder(pointType);
    waypointBuilder = new SimpleFeatureBuilder(waypointType);
    trackBuilder = new SimpleFeatureBuilder(trackType);
    routeBuilder = new SimpleFeatureBuilder(routeType);
    try {
      inputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
      eventReader = inputFactory.createXMLEventReader(fileStream);
      init();
      if (!currentElementStack.isEmpty()) {
        nextFeature = getNext();
      } else {
        nextFeature = null;
      }
    } catch (final Exception e) {
      LOGGER.error("Error processing GPX input stream", e);
      nextFeature = null;
    }
  }

  @Override
  public boolean hasNext() {
    return (nextFeature != null);
  }

  @Override
  public GeoWaveData<SimpleFeature> next() {
    final GeoWaveData<SimpleFeature> ret = nextFeature;
    try {
      nextFeature = getNext();
    } catch (final Exception e) {
      LOGGER.error("Error processing GPX input stream", e);
      nextFeature = null;
    }
    return ret;
  }

  @Override
  public void remove() {}

  @Override
  public void close() {
    try {
      eventReader.close();
    } catch (final Exception e2) {
      LOGGER.warn("Unable to close track XML stream", e2);
    }
    IOUtils.closeQuietly(fileStream);
  }

  private void init() throws IOException, Exception {

    while (eventReader.hasNext()) {
      final XMLEvent event = eventReader.nextEvent();
      if (event.isStartElement()) {
        final StartElement node = event.asStartElement();
        if ("gpx".equals(node.getName().getLocalPart())) {
          currentElementStack.push(top);
          processElementAttributes(node, top);
          return;
        }
      }
    }
  }

  private GeoWaveData<SimpleFeature> getNext() throws Exception {

    GPXDataElement currentElement = currentElementStack.peek();
    GeoWaveData<SimpleFeature> newFeature = null;
    while ((newFeature == null) && eventReader.hasNext()) {
      final XMLEvent event = eventReader.nextEvent();
      if (event.isStartElement()) {
        final StartElement node = event.asStartElement();
        if (!processElementValues(node, currentElement)) {
          final GPXDataElement newElement =
              new GPXDataElement(event.asStartElement().getName().getLocalPart(), maxLength);
          currentElement.addChild(newElement);
          currentElement = newElement;
          currentElementStack.push(currentElement);
          processElementAttributes(node, currentElement);
        }
      } else if (event.isEndElement()
          && event.asEndElement().getName().getLocalPart().equals(currentElement.elementType)) {
        final GPXDataElement child = currentElementStack.pop();
        newFeature = postProcess(child);
        if ((newFeature == null) && !currentElementStack.isEmpty()) {
          currentElement = currentElementStack.peek();
          // currentElement.removeChild(child);
        } else if (currentElementStack.size() == 1) {
          top.children.remove(child);
        }
      }
    }
    return newFeature;
  }

  private String getChildCharacters(final XMLEventReader eventReader, final String elType)
      throws Exception {
    final StringBuilder buf = new StringBuilder();
    XMLEvent event = eventReader.nextEvent();
    while (!(event.isEndElement()
        && event.asEndElement().getName().getLocalPart().equals(elType))) {
      if (event.isCharacters()) {
        buf.append(event.asCharacters().getData());
      }
      event = eventReader.nextEvent();
    }
    return buf.toString().trim();
  }

  private void processElementAttributes(final StartElement node, final GPXDataElement element)
      throws Exception {
    @SuppressWarnings("unchecked")
    final Iterator<Attribute> attributes = node.getAttributes();
    while (attributes.hasNext()) {
      final Attribute a = attributes.next();
      if (a.getName().getLocalPart().equals("lon")) {
        element.lon = Double.parseDouble(a.getValue());
      } else if (a.getName().getLocalPart().equals("lat")) {
        element.lat = Double.parseDouble(a.getValue());
      }
    }
  }

  private boolean processElementValues(final StartElement node, final GPXDataElement element)
      throws Exception {
    switch (node.getName().getLocalPart()) {
      case "ele": {
        element.elevation = Double.parseDouble(getChildCharacters(eventReader, "ele"));
        break;
      }
      case "magvar": {
        element.magvar = Double.parseDouble(getChildCharacters(eventReader, "magvar"));
        break;
      }
      case "geoidheight": {
        element.geoidheight = Double.parseDouble(getChildCharacters(eventReader, "geoidheight"));
        break;
      }
      case "name": {
        element.name = getChildCharacters(eventReader, "name");
        break;
      }
      case "cmt": {
        element.cmt = getChildCharacters(eventReader, "cmt");
        break;
      }
      case "desc": {
        element.desc = getChildCharacters(eventReader, "desc");
        break;
      }
      case "src": {
        element.src = getChildCharacters(eventReader, "src");
        break;
      }
      case "link": {
        element.link = getChildCharacters(eventReader, "link");
        break;
      }
      case "sym": {
        element.sym = getChildCharacters(eventReader, "sym");
        break;
      }
      case "type": {
        element.type = getChildCharacters(eventReader, "type");
        break;
      }
      case "sat": {
        element.sat = Integer.parseInt(getChildCharacters(eventReader, "sat"));
        break;
      }
      case "dgpsid": {
        element.dgpsid = Integer.parseInt(getChildCharacters(eventReader, "dgpsid"));
        break;
      }
      case "vdop": {
        element.vdop = Double.parseDouble(getChildCharacters(eventReader, "vdop"));
        break;
      }
      case "fix": {
        element.fix = getChildCharacters(eventReader, "fix");
        break;
      }
      case "course": {
        element.course = Double.parseDouble(getChildCharacters(eventReader, "course"));
        break;
      }
      case "speed": {
        element.speed = Double.parseDouble(getChildCharacters(eventReader, "speed"));
        break;
      }
      case "hdop": {
        element.hdop = Double.parseDouble(getChildCharacters(eventReader, "hdop"));
        break;
      }
      case "pdop": {
        element.pdop = Double.parseDouble(getChildCharacters(eventReader, "pdop"));
        break;
      }
      case "url": {
        element.url = getChildCharacters(eventReader, "url");
        break;
      }
      case "number": {
        element.number = getChildCharacters(eventReader, "number");
        break;
      }
      case "urlname": {
        element.urlname = getChildCharacters(eventReader, "urlname");
        break;
      }
      case "time": {
        try {
          element.timestamp =
              GpxUtils.parseDateSeconds(getChildCharacters(eventReader, "time")).getTime();

        } catch (final ParseException e) {
          LOGGER.warn("Unable to parse date in seconds", e);
          try {
            element.timestamp =
                GpxUtils.parseDateMillis(getChildCharacters(eventReader, "time")).getTime();
          } catch (final ParseException e2) {
            LOGGER.warn("Unable to parse date in millis", e2);
          }
        }
        break;
      }
      default:
        return false;
    }
    return true;
  }

  private static class GPXDataElement {

    Long timestamp = null;
    Integer dgpsid = null;
    Double elevation = null;
    Double lat = null;
    Double lon = null;
    Double course = null;
    Double speed = null;
    Double magvar = null;
    Double geoidheight = null;
    String name = null;
    String cmt = null;
    String desc = null;
    String src = null;
    String fix = null;
    String link = null;
    String sym = null;
    String type = null;
    String url = null;
    String urlname = null;
    Integer sat = null;
    Double hdop = null;
    Double pdop = null;
    Double vdop = null;
    String elementType;
    // over-rides id
    String number = null;

    Coordinate coordinate = null;
    List<GPXDataElement> children = null;
    GPXDataElement parent;
    long id = 0;
    int childIdCounter = 0;

    double maxLineLength = Double.MAX_VALUE;

    public GPXDataElement(final String myElType) {
      elementType = myElType;
    }

    public GPXDataElement(final String myElType, final double maxLength) {
      elementType = myElType;
      maxLineLength = maxLength;
    }

    @Override
    public String toString() {
      return elementType;
    }

    public String getPath() {
      final StringBuffer buf = new StringBuffer();
      GPXDataElement currentGP = parent;
      buf.append(elementType);
      while (currentGP != null) {
        buf.insert(0, '.');
        buf.insert(0, currentGP.elementType);
        currentGP = currentGP.parent;
      }
      return buf.toString();
    }

    public void addChild(final GPXDataElement child) {

      if (children == null) {
        children = new ArrayList<>();
      }
      children.add(child);
      child.parent = this;
      child.id = ++childIdCounter;
    }

    public String composeID(
        final String prefix,
        final boolean includeLatLong,
        final boolean includeParent) {
      // /top?
      if (parent == null) {
        if ((prefix != null) && (prefix.length() > 0)) {
          return prefix;
        }
      }

      final StringBuffer buf = new StringBuffer();
      if ((parent != null) && includeParent) {
        final String parentID = parent.composeID(prefix, false, true);
        if (parentID.length() > 0) {
          buf.append(parentID);
          buf.append('_');
        }
        if ((number != null) && (number.length() > 0)) {
          buf.append(number);
        } else {
          buf.append(id);
        }
        buf.append('_');
      }
      if ((name != null) && (name.length() > 0)) {
        buf.append(name.replaceAll("\\s+", "_"));
        buf.append('_');
      }
      if (includeLatLong && (lat != null) && (lon != null)) {
        buf.append(toID(lat)).append('_').append(toID(lon));
        buf.append('_');
      }
      if (buf.length() > 0) {
        buf.deleteCharAt(buf.length() - 1);
      }
      return buf.toString();
    }

    public Coordinate getCoordinate() {
      if (coordinate != null) {
        return coordinate;
      }
      if ((lat != null) && (lon != null)) {
        coordinate = new Coordinate(lon, lat);
      }
      return coordinate;
    }

    public boolean isCoordinate() {
      return (lat != null) && (lon != null);
    }

    public List<Coordinate> buildCoordinates() {
      if (isCoordinate()) {
        return Arrays.asList(getCoordinate());
      }
      final ArrayList<Coordinate> coords = new ArrayList<>();
      for (int i = 0; (children != null) && (i < children.size()); i++) {
        coords.addAll(children.get(i).buildCoordinates());
      }
      return coords;
    }

    private Long getStartTime() {
      if (children == null) {
        return timestamp;
      }
      long minTime = Long.MAX_VALUE;
      for (final GPXDataElement element : children) {
        final Long t = element.getStartTime();
        if (t != null) {
          minTime = Math.min(t.longValue(), minTime);
        }
      }
      return (minTime < Long.MAX_VALUE) ? Long.valueOf(minTime) : null;
    }

    private Long getEndTime() {
      if (children == null) {
        return timestamp;
      }
      long maxTime = 0;
      for (final GPXDataElement element : children) {
        final Long t = element.getEndTime();
        if (t != null) {
          maxTime = Math.max(t.longValue(), maxTime);
        }
      }
      return (maxTime > 0) ? Long.valueOf(maxTime) : null;
    }

    public boolean build(
        final SimpleFeatureBuilder builder,
        final Long backupTimestamp,
        final boolean timeRange) {
      if ((lon != null) && (lat != null)) {
        final Coordinate p = getCoordinate();
        builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
        builder.set("Latitude", lat);
        builder.set("Longitude", lon);
      }
      setAttribute(builder, "Elevation", elevation);
      setAttribute(builder, "Course", course);
      setAttribute(builder, "Speed", speed);
      setAttribute(builder, "Source", src);
      setAttribute(builder, "Link", link);
      setAttribute(builder, "URL", url);
      setAttribute(builder, "URLName", urlname);
      setAttribute(builder, "MagneticVariation", magvar);
      setAttribute(builder, "Satellites", sat);
      setAttribute(builder, "Symbol", sym);
      setAttribute(builder, "VDOP", vdop);
      setAttribute(builder, "HDOP", hdop);
      setAttribute(builder, "GeoHeight", geoidheight);
      setAttribute(builder, "Fix", fix);
      setAttribute(builder, "Station", dgpsid);
      setAttribute(builder, "PDOP", pdop);
      setAttribute(builder, "Classification", type);
      setAttribute(builder, "Name", name);
      setAttribute(builder, "Comment", cmt);
      setAttribute(builder, "Description", desc);
      setAttribute(builder, "Symbol", sym);
      if (timestamp != null) {
        setAttribute(builder, "Timestamp", new Date(timestamp));
      } else if ((backupTimestamp != null) && !timeRange) {
        setAttribute(builder, "Timestamp", new Date(backupTimestamp));
      }
      if (children != null) {

        boolean setDuration = true;

        final List<Coordinate> childSequence = buildCoordinates();

        final int childCoordCount = childSequence.size();
        if (childCoordCount <= 1) {
          return false;
        }

        final LineString geom =
            GeometryUtils.GEOMETRY_FACTORY.createLineString(
                childSequence.toArray(new Coordinate[childSequence.size()]));

        // Filter gpx track based on maxExtent
        if (geom.isEmpty() || (geom.getEnvelopeInternal().maxExtent() > maxLineLength)) {
          return false;
        }

        builder.set("geometry", geom);

        setAttribute(builder, "NumberPoints", Long.valueOf(childCoordCount));

        Long minTime = getStartTime();
        if (minTime != null) {
          builder.set("StartTimeStamp", new Date(minTime));
        } else if ((timestamp != null) && timeRange) {
          minTime = timestamp;
          builder.set("StartTimeStamp", new Date(timestamp));
        } else if ((backupTimestamp != null) && timeRange) {
          minTime = backupTimestamp;
          builder.set("StartTimeStamp", new Date(backupTimestamp));
        } else {
          setDuration = false;
        }
        Long maxTime = getEndTime();
        if (maxTime != null) {
          builder.set("EndTimeStamp", new Date(maxTime));
        } else if ((timestamp != null) && timeRange) {
          maxTime = timestamp;
          builder.set("EndTimeStamp", new Date(timestamp));
        } else if ((backupTimestamp != null) && timeRange) {
          maxTime = backupTimestamp;
          builder.set("EndTimeStamp", new Date(backupTimestamp));
        } else {
          setDuration = false;
        }
        if (setDuration) {
          builder.set("Duration", maxTime - minTime);
        }
      }
      return true;
    }
  }

  private GeoWaveData<SimpleFeature> postProcess(final GPXDataElement element) {

    switch (element.elementType) {
      case "trk": {
        if ((element.children != null) && element.build(trackBuilder, backupTimestamp, true)) {
          trackBuilder.set(
              "TrackId",
              inputID.length() > 0 ? inputID : element.composeID("", false, true));
          return buildGeoWaveDataInstance(
              element.composeID(inputID, false, true),
              indexNames,
              GpxUtils.GPX_TRACK_FEATURE,
              trackBuilder,
              additionalData.get(element.getPath()));
        }
        break;
      }
      case "rte": {
        if ((element.children != null) && element.build(routeBuilder, backupTimestamp, true)) {
          trackBuilder.set(
              "TrackId",
              inputID.length() > 0 ? inputID : element.composeID("", false, true));
          return buildGeoWaveDataInstance(
              element.composeID(inputID, false, true),
              indexNames,
              GpxUtils.GPX_ROUTE_FEATURE,
              routeBuilder,
              additionalData.get(element.getPath()));
        }
        break;
      }
      case "wpt": {
        if (element.build(waypointBuilder, backupTimestamp, false)) {
          return buildGeoWaveDataInstance(
              element.composeID(uniqueWayPoints ? "" : inputID, true, !uniqueWayPoints),
              indexNames,
              GpxUtils.GPX_WAYPOINT_FEATURE,
              waypointBuilder,
              additionalData.get(element.getPath()));
        }
        break;
      }
      case "rtept": {
        if (element.build(waypointBuilder, backupTimestamp, false)) {
          return buildGeoWaveDataInstance(
              element.composeID(inputID, true, true),
              indexNames,
              GpxUtils.GPX_WAYPOINT_FEATURE,
              waypointBuilder,
              additionalData.get(element.getPath()));
        }
        break;
      }
      case "trkseg": {
        break;
      }
      case "trkpt": {
        if (element.build(pointBuilder, backupTimestamp, false)) {
          if ((element.timestamp == null) && (backupTimestamp == null)) {
            pointBuilder.set("Timestamp", null);
          }
          return buildGeoWaveDataInstance(
              element.composeID(inputID, false, true),
              indexNames,
              GpxUtils.GPX_POINT_FEATURE,
              pointBuilder,
              additionalData.get(element.getPath()));
        }
        break;
      }
    }
    return null;
  }

  private static void setAttribute(
      final SimpleFeatureBuilder builder,
      final String name,
      final Object obj) {
    if ((builder.getFeatureType().getDescriptor(name) != null) && (obj != null)) {
      builder.set(name, obj);
    }
  }

  private static GeoWaveData<SimpleFeature> buildGeoWaveDataInstance(
      final String id,
      final String[] indexNames,
      final String key,
      final SimpleFeatureBuilder builder,
      final Map<String, String> additionalDataSet) {

    if (additionalDataSet != null) {
      for (final Map.Entry<String, String> entry : additionalDataSet.entrySet()) {
        builder.set(entry.getKey(), entry.getValue());
      }
    }
    return new GeoWaveData<>(key, indexNames, builder.buildFeature(id));
  }

  private static String toID(final Double val) {
    return LatLongFormat.format(val.doubleValue() * 10000000);
  }
}
