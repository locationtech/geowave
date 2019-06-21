/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser;

import java.awt.Color;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import javax.vecmath.Point2d;
import javax.vecmath.Point3d;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.IllegalAddException;
import org.jdom.IllegalDataException;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.Verifier;
import org.jdom.filter.ElementFilter;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JDOMUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDOMUtils.class);

  public static final String tagLayerBounds = "LayerBounds";
  public static final String tagX = "X";
  public static final String tagY = "Y";
  public static final String tagZ = "Z";
  public static final String tagLat = "Lat";
  public static final String tagLon = "Lon";
  public static final String tagAlt = "Alt";
  public static final String tagLL = "LL";
  public static final String tagUR = "UR";
  public static final String tagStart = "start";
  public static final String tagStop = "stop";

  public static Element parseDocument(final URL docUrl) {
    try {
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      builder.setValidation(false);

      final Document doc = builder.build(docUrl);
      if (doc == null) {
        return null;
      }

      final Element root = doc.getRootElement();
      return root;
    } catch (final IOException ioe) {
      LOGGER.warn("parse error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.warn("parse error", jdome);
      return null;
    }
  }

  public static Element parseDocument(final File f) {
    try {
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      final Document doc = builder.build(f);
      if (doc == null) {
        return null;
      }

      final Element root = doc.getRootElement();
      return root;
    } catch (final IOException ioe) {
      LOGGER.warn("parse error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.warn("parse error", jdome);
      return null;
    }
  }

  public static Element parseDocument(final InputStream is) {
    try {
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

      final Document doc = builder.build(is);
      if (doc == null) {
        return null;
      }

      final Element root = doc.getRootElement();
      return root;
    } catch (final IOException ioe) {
      LOGGER.warn("parse error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.warn("parse error", jdome);
      return null;
    }
  }

  public static Element parseDocument(final InputSource is) {
    try {
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      final Document doc = builder.build(is);
      if (doc == null) {
        return null;
      }

      final Element root = doc.getRootElement();
      return root;
    } catch (final IOException ioe) {
      LOGGER.warn("parse error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.warn("parse error", jdome);
      return null;
    }
  }

  public static Element parseDocument(final String filename) {
    final File f = new File(filename);
    return parseDocument(f);
  }

  public static void writeElementToStream(final Element e, final OutputStream os) {
    try {
      final BufferedOutputStream bos = new BufferedOutputStream(os);

      final Document document = new Document((Element) e.clone());
      final XMLOutputter outputter = new XMLOutputter();

      outputter.output(document, bos);

      bos.flush();
    } catch (final IOException ioe) {
      LOGGER.info("write error", ioe);
    }
  }

  public static void writeElementToStreamPretty(final Element e, final OutputStream os) {
    try {
      final BufferedOutputStream bos = new BufferedOutputStream(os);

      final Document document = new Document((Element) e.clone());
      final XMLOutputter outputter = new XMLOutputter(Format.getPrettyFormat());

      outputter.output(document, bos);

      bos.flush();
    } catch (final IOException ioe) {
      LOGGER.info("write error", ioe);
    }
  }

  public static String writeElementToString(final Element e) {
    try {
      final StringWriter sw = new StringWriter();
      final Document document = new Document((Element) e.clone());
      final XMLOutputter outputter = new XMLOutputter();

      outputter.output(document, sw);

      return sw.getBuffer().toString();
    } catch (final IOException ioe) {
      LOGGER.info("write error", ioe);
    }

    return null;
  }

  public static String writeElementToStringWithoutHeader(final Element e) {
    try {
      final StringWriter sw = new StringWriter();
      final XMLOutputter outputter = new XMLOutputter();

      outputter.output(e, sw);

      return sw.getBuffer().toString();
    } catch (final IOException ioe) {
      LOGGER.info("write error", ioe);
    }

    return null;
  }

  public static void writeElementToWriter(final Element e, final Writer writer) {
    try {
      final Document document = new Document((Element) e.clone());
      final XMLOutputter outputter = new XMLOutputter();

      outputter.output(document, writer);
    } catch (final IOException ioe) {
      LOGGER.info("write error", ioe);
    }
  }

  public static Document readDocumentFromString(final String xmlData) {
    try {
      final StringReader sr = new StringReader(xmlData);
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      final Document doc = builder.build(sr);
      return doc;
    } catch (final IOException ioe) {
      LOGGER.info("read error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.info("read error", jdome);
      return null;
    }
  }

  public static Element readElementFromString(final String xmlData) {
    try {
      final StringReader sr = new StringReader(xmlData);
      final SAXBuilder builder = new SAXBuilder();
      builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      final Document doc = builder.build(sr);

      if (doc == null) {
        return null;
      }

      final Element root = doc.getRootElement();
      return root;
    } catch (final IOException ioe) {
      LOGGER.info("read error", ioe);
      return null;
    } catch (final JDOMException jdome) {
      LOGGER.info("read error", jdome);
      return null;
    }
  }

  public static String getProp(
      final Element e,
      final String childName,
      final Logger logger,
      final String errorMessage) {
    final Element childEl = e.getChild(childName);
    if (childEl == null) {
      logger.error(errorMessage);
      return null;
    }

    final String val = childEl.getTextTrim();

    if (val == null) {
      logger.error(errorMessage);
      return null;
    }

    return val;
  }

  public static Element writeElementList(final String tag, final Collection<?> c) {
    final Element el = new Element(tag);
    try {
      el.addContent(c);
    } catch (final IllegalAddException e) {
      LOGGER.warn(e + ":  " + el.toString(), e);
    }
    return el;
  }

  public static Element writeElementList(
      final String tag,
      final Collection<?> c,
      final Namespace ns) {
    final Element e = new Element(tag, ns);
    e.addContent(c);
    return e;
  }

  public static Element writeElement(final String tag, final Element childElm) {
    final Element e = new Element(tag);
    e.addContent(childElm);
    return e;
  }

  public static Element writeElement(final String tag, final Element childElm, final Namespace ns) {
    final Element e = new Element(tag, ns);
    e.addContent(childElm);
    return e;
  }

  public static Element writeStringVal(final String tag, final String val) {
    final Element e = new Element(tag);
    addSanitizedContent(e, val);
    return e;
  }

  public static Element writeStringVal(final String tag, final String val, final Namespace ns) {
    final Element e = new Element(tag, ns);
    addSanitizedContent(e, val);
    return e;
  }

  private static void addSanitizedContent(final Element e, final String val) {
    try {
      e.addContent(val);
    } catch (final IllegalDataException ide) {
      LOGGER.warn("Unable to add content", ide);
      // Unless a better idea can be found, we need to replace all
      // unparseable characters with a space as a placeholder
      final StringBuffer newVal = new StringBuffer();
      for (int i = 0, len = val.length(); i < len; i++) {
        if (Verifier.isXMLCharacter(val.charAt(i))) {
          newVal.append(val.charAt(i));
        } else {
          newVal.append(' ');
        }
      }
      e.addContent(newVal.toString());
    }
  }

  public static String getStringVal(final Element e, final String childText, final Namespace ns) {
    if (e == null) {
      return null;
    } else {
      return e.getChildTextTrim(childText, ns);
    }
  }

  public static String getStringVal(final Element e) {
    return getStringVal(e, true);
  }

  public static String getStringVal(final Element e, final boolean trim) {
    if (e == null) {
      return null;
    } else {
      if (trim) {
        return e.getTextTrim();
      } else {
        return e.getText();
      }
    }
  }

  public static String getStringVal(final Element e, final String childText) {
    return getStringVal(e, childText, true);
  }

  public static String getStringVal(final Element e, final String childText, final boolean trim) {
    if (e == null) {
      return null;
    } else {
      if (trim) {
        return e.getChildTextTrim(childText);
      } else {
        return e.getChildText(childText);
      }
    }
  }

  public static Element writeEmptyProperty(final String tag) {
    final Element e = new Element(tag);
    return e;
  }

  public static Element writeDoubleVal(final String tag, final double d) {
    return writeStringVal(tag, Double.toString(d));
  }

  public static Element writeDoubleVal(final String tag, final double d, final Namespace ns) {
    return writeStringVal(tag, Double.toString(d), ns);
  }

  public static Double getDoubleVal(final Element e) {
    try {
      return Double.valueOf(e.getText());
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Double getDoubleVal(
      final Element e,
      final String childText,
      final double defaultValue) {
    final Double value = getDoubleVal(e, childText);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static Double getDoubleVal(final Element e, final String childText) {
    try {
      return Double.valueOf(e.getChildText(childText));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Double getDoubleVal(final Element e, final String childText, final Namespace ns) {
    try {
      return Double.valueOf(e.getChildText(childText, ns));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Double getAttrDoubleVal(final Element e, final String attrName) {
    try {
      return Double.valueOf(e.getAttributeValue(attrName));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Element writeFloatVal(final String tag, final float f) {
    return writeStringVal(tag, Float.toString(f));
  }

  public static Float getFloatVal(final Element e, final String childText) {
    final String str = getStringVal(e, childText);

    Float val = null;

    if (str != null) {
      try {
        val = Float.parseFloat(str);
      } catch (final Exception ex) {
        LOGGER.warn("Unable to get parse", ex);
      }
    }

    return val;
  }

  public static Element writeIntegerVal(final String tag, final int i) {
    return writeStringVal(tag, Integer.toString(i));
  }

  public static Element writeIntegerVal(final String tag, final int i, final Namespace ns) {
    return writeStringVal(tag, Integer.toString(i), ns);
  }

  public static Element writeShortVal(final String tag, final short s) {
    return writeStringVal(tag, Short.toString(s));
  }

  public static Element writeShortVal(final String tag, final short s, final Namespace ns) {
    return writeStringVal(tag, Short.toString(s), ns);
  }

  public static Short getShortVal(final Element e) {
    try {
      return Short.valueOf(e.getText());
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Short getShortVal(
      final Element e,
      final String childText,
      final short defaultValue) {
    final Short value = getShortVal(e, childText);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static Short getShortVal(final Element e, final String childText) {
    try {
      return Short.valueOf(e.getChildText(childText));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Short getShortVal(final Element e, final String childText, final Namespace ns) {
    try {
      return Short.valueOf(e.getChildText(childText, ns));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Element writeByteVal(final String tag, final byte b) {
    return writeStringVal(tag, Byte.toString(b));
  }

  public static Element writeByteVal(final String tag, final byte b, final Namespace ns) {
    return writeStringVal(tag, Byte.toString(b), ns);
  }

  public static Byte getByteVal(final Element e) {
    try {
      return Byte.valueOf(e.getText());
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Byte getByteVal(final Element e, final String childText) {
    try {
      return Byte.valueOf(e.getChildText(childText));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Byte getByteVal(final Element e, final String childText, final Namespace ns) {
    try {
      return Byte.valueOf(e.getChildText(childText, ns));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Integer getIntegerVal(final Element e) {
    try {
      return Integer.valueOf(e.getText());
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Integer getIntegerVal(
      final Element e,
      final String childText,
      final int defaultValue) {
    final Integer value = getIntegerVal(e, childText);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static Integer getIntegerVal(final Element e, final String childText) {
    try {
      return Integer.valueOf(e.getChildText(childText));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Integer getIntegerVal(final Element e, final String childText, final Namespace ns) {
    try {
      return Integer.valueOf(e.getChildText(childText, ns));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Element writeLongVal(final String tag, final long i) {
    return writeStringVal(tag, Long.toString(i));
  }

  public static Element writeLongVal(final String tag, final long i, final Namespace ns) {
    return writeStringVal(tag, Long.toString(i), ns);
  }

  public static Long getLongVal(final Element e) {
    try {
      return Long.valueOf(e.getText());
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Long getLongVal(final Element e, final String childText, final long defaultValue) {
    final Long value = getLongVal(e, childText);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  public static Long getLongVal(final Element e, final String childTag) {
    try {
      return Long.valueOf(e.getChildText(childTag));
    } catch (final Exception ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Long getLongVal(final Element e, final String childText, final Namespace ns) {
    try {
      return Long.valueOf(e.getChildText(childText, ns));
    } catch (final NumberFormatException ex) {
      LOGGER.error("Unable to get value", ex);
      return null;
    }
  }

  public static Element writeBooleanVal(final String tag, final Boolean b) {
    if (b == null) {
      return writeStringVal(tag, "");
    }
    return writeStringVal(tag, Boolean.toString(b));
  }

  public static boolean getBooleanVal(
      final Element e,
      final String childTag,
      final boolean defaultValue) {
    if ((e == null) || (e.getChildText(childTag) == null)) {
      return defaultValue;
    }

    final Boolean value = getBooleanVal(e, childTag);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  @SuppressFBWarnings(
      value = "NP_BOOLEAN_RETURN_NULL",
      justification = "its private and only used by methods that check for null")
  private static Boolean getBooleanVal(final Element e, final String childTag) {
    final String text = e.getChildText(childTag);

    if ((text == null) || (text.isEmpty())) {
      return null;
    }

    return Boolean.valueOf(text.trim());
  }

  public static boolean getBooleanVal(final Element e, final boolean defaultValue) {
    if ((e == null) || (e.getText() == null)) {
      return defaultValue;
    }

    final Boolean value = getBooleanVal(e);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  @SuppressFBWarnings(
      value = "NP_BOOLEAN_RETURN_NULL",
      justification = "its private and only used by methods that check for null")
  private static Boolean getBooleanVal(final Element e) {
    final String text = e.getText();

    if ((text == null) || (text.isEmpty())) {
      return null;
    }

    return Boolean.valueOf(text);
  }

  public static Element getElementVal(final Element e, final String tag) {
    return e.getChild(tag);
  }

  public static Point2d readPoint(final String tagName, final Element parentEl) {
    final Element ptEl = parentEl.getChild(tagName);

    if (ptEl == null) {
      return null;
    } else {
      final double lat = getDoubleVal(ptEl, JDOMUtils.tagLat);
      final double lon = getDoubleVal(ptEl, JDOMUtils.tagLon);

      return new Point2d(lon, lat);
    }
  }

  public static Point2d readPoint(final Element ptEl) {
    if (ptEl == null) {
      return null;
    } else {
      final double lat = getDoubleVal(ptEl, JDOMUtils.tagLat);
      final double lon = getDoubleVal(ptEl, JDOMUtils.tagLon);

      return new Point2d(lon, lat);
    }
  }

  public static Element writePoint(final String tagName, final Point2d pt) {
    final ArrayList<Element> v = new ArrayList<>();

    v.add(writeDoubleVal(JDOMUtils.tagLat, pt.y));

    v.add(writeDoubleVal(JDOMUtils.tagLon, pt.x));

    return writeElementList(tagName, v);
  }

  public static Element writePointList(final String tagName, final ArrayList<Point2d> pts) {
    final StringBuffer sb = new StringBuffer();

    final int nPts = pts.size();
    int idx = 0;
    for (final Point2d pt : pts) {
      sb.append(Double.toString(pt.x));
      sb.append(",");
      sb.append(Double.toString(pt.y));

      if (idx < (nPts - 1)) {
        sb.append(",");
      }

      idx++;
    }

    return writeStringVal(tagName, sb.toString());
  }

  public static ArrayList<Point2d> readPointList(final Element el) {
    final ArrayList<Point2d> pts = new ArrayList<>();

    final String ptStr = getStringVal(el);
    final StringTokenizer st = new StringTokenizer(ptStr, ",");
    while (st.hasMoreTokens()) {
      try {
        final String xStr = st.nextToken();
        final String yStr = st.nextToken();

        final double x = Double.parseDouble(xStr);
        final double y = Double.parseDouble(yStr);

        pts.add(new Point2d(x, y));
      } catch (final Exception e) {
        LOGGER.warn("error parsing point list", e);

        return null;
      }
    }

    return pts;
  }

  public static Element writePoint3dList(final String tagName, final ArrayList<Point3d> pts) {
    if (pts == null) {
      return null;
    } else {
      final StringBuffer sb = new StringBuffer();

      final int nPts = pts.size();
      int idx = 0;
      for (final Point3d pt : pts) {
        sb.append(Double.toString(pt.x));
        sb.append(",");
        sb.append(Double.toString(pt.y));
        sb.append(",");
        sb.append(Double.toString(pt.z));

        if (idx < (nPts - 1)) {
          sb.append(",");
        }

        idx++;
      }

      return writeStringVal(tagName, sb.toString());
    }
  }

  public static ArrayList<Point3d> readPoint3dList(final Element parentEl, final String tagName) {
    final Element el = parentEl.getChild(tagName);

    if (el == null) {
      return null;
    } else {
      final ArrayList<Point3d> pts = new ArrayList<>();

      final String ptStr = getStringVal(el);
      final StringTokenizer st = new StringTokenizer(ptStr, ",");
      while (st.hasMoreTokens()) {
        try {
          final String xStr = st.nextToken();
          final String yStr = st.nextToken();
          final String zStr = st.nextToken();

          final double x = Double.parseDouble(xStr);
          final double y = Double.parseDouble(yStr);
          final double z = Double.parseDouble(zStr);

          pts.add(new Point3d(x, y, z));
        } catch (final Exception e) {
          LOGGER.warn("error parsing point list", e);

          return null;
        }
      }

      return pts;
    }
  }

  public static Element writePoint2d(final String tagName, final Point2d pt) {
    final ArrayList<Element> v = new ArrayList<>();

    v.add(writeDoubleVal(JDOMUtils.tagX, pt.x));

    v.add(writeDoubleVal(JDOMUtils.tagY, pt.y));

    return writeElementList(tagName, v);
  }

  public static Point2d readPoint2d(final String tagName, final Element parentEl) {
    final Element ptEl = parentEl.getChild(tagName);

    if (ptEl == null) {
      return null;
    } else {
      final double x = getDoubleVal(ptEl, JDOMUtils.tagX);
      final double y = getDoubleVal(ptEl, JDOMUtils.tagY);

      return new Point2d(x, y);
    }
  }

  public static Point2d readPoint2d(final Element ptEl) {
    if (ptEl == null) {
      return null;
    } else {
      final double x = getDoubleVal(ptEl, JDOMUtils.tagX);
      final double y = getDoubleVal(ptEl, JDOMUtils.tagY);

      return new Point2d(x, y);
    }
  }

  public static Element writePoint3d(final String tagName, final Point3d pt) {
    final ArrayList<Element> v = new ArrayList<>();

    v.add(writeDoubleVal(JDOMUtils.tagX, pt.x));

    v.add(writeDoubleVal(JDOMUtils.tagY, pt.y));

    v.add(writeDoubleVal(JDOMUtils.tagZ, pt.z));

    return writeElementList(tagName, v);
  }

  public static Point3d readPoint3d(final String tagName, final Element parentEl) {
    final Element ptEl = parentEl.getChild(tagName);

    if (ptEl == null) {
      return null;
    } else {
      final double x = getDoubleVal(ptEl, JDOMUtils.tagX);
      final double y = getDoubleVal(ptEl, JDOMUtils.tagY);
      final double z = getDoubleVal(ptEl, JDOMUtils.tagZ);

      return new Point3d(x, y, z);
    }
  }

  public static Point3d readPoint3d(final Element ptEl) {
    if (ptEl == null) {
      return null;
    } else {
      final double x = getDoubleVal(ptEl, JDOMUtils.tagX);
      final double y = getDoubleVal(ptEl, JDOMUtils.tagY);
      final double z = getDoubleVal(ptEl, JDOMUtils.tagZ);

      return new Point3d(x, y, z);
    }
  }

  public static Point2d[] readBounds(final String tagName, final Element parentEl) {
    final Element boundsEl = parentEl.getChild(tagName);

    if (boundsEl == null) {
      // logger.info("missing bounds element '" + tagName + "'");
      return null;
    } else {
      return readBounds(boundsEl);
    }
  }

  public static Point2d[] readBounds(final Element boundsEl) {
    if (boundsEl == null) {
      return null;
    }

    final Point2d ll = readPoint(JDOMUtils.tagLL, boundsEl);

    if (ll == null) {
      return null;
    }

    final Point2d ur = readPoint(JDOMUtils.tagUR, boundsEl);

    final Point2d[] bounds = new Point2d[2];
    bounds[0] = ll;
    bounds[1] = ur;

    return bounds;
  }

  public static Element writeBounds(final String tagName, final Point2d[] bounds) {
    final ArrayList<Element> v = new ArrayList<>();

    if ((bounds != null) && (bounds[0] != null)) {
      v.add(writePoint(JDOMUtils.tagLL, bounds[0]));

      v.add(writePoint(JDOMUtils.tagUR, bounds[1]));
    }

    return writeElementList(tagName, v);
  }

  public static ArrayList<Double> readDoubleList(final String tagName, final Element elList) {
    final List<?> doubleElementList = elList.getChildren(tagName);
    final ArrayList<Double> doubleList = new ArrayList<>();

    for (final Object doubleElement : doubleElementList) {
      doubleList.add(getDoubleVal((Element) doubleElement));
    }

    return doubleList;
  }

  public static ArrayList<Element> writeDoubleList(
      final String tagName,
      final List<Double> doubleList) {
    final ArrayList<Element> doubleElementList = new ArrayList<>();

    for (final Double doubleVal : doubleList) {
      doubleElementList.add(writeDoubleVal(tagName, doubleVal));
    }

    return doubleElementList;
  }

  public static ArrayList<Color> readColorList(final String tagName, final Element elList) {
    final List<?> colorElementList = elList.getChildren(tagName);
    final ArrayList<Color> colorList = new ArrayList<>();

    for (final Object colorElement : colorElementList) {
      colorList.add(readColor((Element) colorElement));
    }

    return colorList;
  }

  public static ArrayList<Element> writeColorList(
      final String tagName,
      final List<Color> colorList) {
    final ArrayList<Element> colorElementList = new ArrayList<>();

    for (final Color color : colorList) {
      colorElementList.add(writeColor(tagName, color));
    }

    return colorElementList;
  }

  public static Date readDate(final String tagName, final Element parentEl) {
    final Element boundsEl = parentEl.getChild(tagName);

    if (boundsEl == null) {
      // logger.info("missing date element '" + tagName + "'");
      return null;
    } else {
      final Long startL = getLongVal(boundsEl);

      if (startL == null) {
        return null;
      } else {
        return new Date(startL);
      }
    }
  }

  public static Element writeDate(final String tagName, final Date date) {
    if (date != null) {
      return writeLongVal(tagName, date.getTime());
    } else {
      return null;
    }
  }

  public static Element writeColor(final String tagName, final Color c) {
    return writeIntegerVal(tagName, c.getRGB());
  }

  public static Color readColor(final String tagName, final Element el) {
    if (el == null) {
      return null;
    } else {
      final Integer colVal = getIntegerVal(el.getChild(tagName));
      if (colVal == null) {
        return null;
      }

      return new Color(colVal, true);
    }
  }

  public static String readStringVal(final String tag, final Element el) {
    if (el == null) {
      return null;
    } else {
      final String strVal = el.getChildText(tag);
      return strVal;
    }
  }

  /**
   * Warning: Can return null!
   *
   * @param tag
   * @param el
   * @return Boolean
   */
  @SuppressFBWarnings(
      value = "NP_BOOLEAN_RETURN_NULL",
      justification = "its known that it can return null")
  public static Boolean readBooleanVal(final String tag, final Element el) {
    if (el == null) {
      return null;
    } else {

      final Boolean boolVal = getBooleanVal(el, tag);
      return boolVal;
    }
  }

  public static Double readDoubleVal(final String tag, final Element el) {
    if (el == null) {
      return null;
    } else {
      return getDoubleVal(el, tag);
    }
  }

  public static Integer readIntegerVal(final String tag, final Element el) {
    if (el == null) {
      return null;
    } else {
      return getIntegerVal(el, tag);
    }
  }

  public static Color readColor(final Element el) {
    if (el == null) {
      return null;
    } else {
      final int colVal = getIntegerVal(el);
      return new Color(colVal);
    }
  }

  public static Element findFirstChild(final Element parentEl, final String childName) {
    final ElementFilter filter = new ElementFilter(childName);
    final Iterator<Element> childrenIter = parentEl.getDescendants(filter);

    if (childrenIter.hasNext()) {
      return childrenIter.next();
    }

    return null;
  }

  public static Element getChildIgnoreNamespace(
      final Element parentEl,
      final String childName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    Element el = parentEl.getChild(childName);

    if (el == null) {
      for (final Namespace ns : namespaces) {
        el = parentEl.getChild(childName, ns);

        if (el != null) {
          break;
        }
      }
    }

    if ((el == null) && tryLowerCase) {
      el =
          getChildIgnoreNamespace(
              parentEl,
              childName.toLowerCase(Locale.ENGLISH),
              namespaces,
              false);
    }

    return el;
  }

  public static List<Element> getChildrenIgnoreNamespace(
      final Element parentEl,
      final String childName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    List<?> el = parentEl.getChildren(childName);

    if ((el == null) || el.isEmpty()) {
      for (final Namespace ns : namespaces) {
        el = parentEl.getChildren(childName, ns);

        if ((el != null) && (!el.isEmpty())) {
          break;
        }
      }
    }

    if ((el == null) && tryLowerCase) {
      el =
          getChildrenIgnoreNamespace(
              parentEl,
              childName.toLowerCase(Locale.ENGLISH),
              namespaces,
              false);
    }

    if (el == null) {
      return new ArrayList<>();
    }
    final List<Element> elementList = new ArrayList<>();
    for (final Object element : el) {
      elementList.add((Element) element);
    }

    return elementList;
  }

  public static String getStringValIgnoreNamespace(
      final Element parentEl,
      final String childName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    final Element el = getChildIgnoreNamespace(parentEl, childName, namespaces, tryLowerCase);

    if (el != null) {
      return el.getTextTrim();
    } else {
      return null;
    }
  }

  public static Double getDoubleValIgnoreNamespace(
      final Element rootEl,
      final String tagName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    final String str = getStringValIgnoreNamespace(rootEl, tagName, namespaces, tryLowerCase);

    Double val = null;

    if (str != null) {
      try {
        val = Double.parseDouble(str);
      } catch (final NumberFormatException e) {
        LOGGER.error("Unable to parse", e);
      }
    }

    return val;
  }

  public static Float getFloatValIgnoreNamespace(
      final Element rootEl,
      final String tagName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    final String str = getStringValIgnoreNamespace(rootEl, tagName, namespaces, tryLowerCase);

    Float val = null;

    if (str != null) {
      try {
        val = Float.parseFloat(str);
      } catch (final NumberFormatException e) {
        LOGGER.error("Unable to parse", e);
      }
    }

    return val;
  }

  @SuppressFBWarnings(
      value = "NP_BOOLEAN_RETURN_NULL",
      justification = "its known that it can return null")
  public static Boolean getBoolValIgnoreNamespace(
      final Element rootEl,
      final String tagName,
      final Namespace[] namespaces,
      final boolean tryLowerCase) {
    final String str = getStringValIgnoreNamespace(rootEl, tagName, namespaces, tryLowerCase);

    if (str != null) {
      Integer val = null;

      try {
        val = Integer.parseInt(str);
      } catch (final NumberFormatException e) {
        LOGGER.error("Unable to parse", e);
      }

      if (val != null) {
        if (val == 0) {
          return false;
        } else {
          return true;
        }
      }
    }

    return null;
  }

  public static String getAttrStringValIgnoreNamespace(
      final Element resourceEl,
      final String attrName) {
    final List<?> resourceAttr = resourceEl.getAttributes();

    if (resourceAttr != null) {
      for (final Object attrEl : resourceAttr) {
        final Attribute attr = (Attribute) attrEl;
        if (attrName.equalsIgnoreCase(attr.getName())) {
          return attr.getValue();
        }
      }
    }

    return null;
  }

  public static Document string2Doc(final String xml) {
    try {
      final SAXBuilder builder = new SAXBuilder();
      return builder.build(new InputSource(new StringReader(xml)));
    } catch (final JDOMException e) {
      LOGGER.error("Unable to build the SAXBuilder", e);
      return null;
    } catch (final IOException e1) {
      LOGGER.error("Unable to build the SAXBuilder", e1);
      return null;
    }
  }

  public static String doc2String(final Document doc) {
    final StringWriter sw = new StringWriter();
    final Format format = Format.getRawFormat().setEncoding("UTF-8");
    final XMLOutputter xmlOut = new XMLOutputter(format);
    String strOutput = null;

    if (doc != null) {
      try {
        xmlOut.output(doc, sw);
        strOutput = sw.toString();
      } catch (final IOException e) {
        LOGGER.error("Unable to retrieve the xml output", e);
        return null;
      }
    }
    return strOutput;
  }
}
