/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.xml;

import java.io.IOException;

import net.sf.hajdbc.util.Strings;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * @author Paul Ferraro
 */
public class PropertyReplacementFilterTest
{
   private IMocksControl control;
   private ContentHandler contentHandler;
   private DTDHandler dtdHandler;
   private ErrorHandler errorHandler;
   private EntityResolver resolver;
   private XMLReader parent;
   private XMLFilterImpl filter;
   
   private static String oldExistingValue;
   private static String oldNonExistingValue;
   
   @BeforeClass
   public static void beforeClass()
   {
      oldExistingValue = System.setProperty("existing", "Release");
      oldNonExistingValue = System.clearProperty("non-existing");
   }

   @AfterClass
   public static void afterClass()
   {
      if (oldExistingValue != null)
      {
         System.setProperty("existing", oldExistingValue);
      }
      else
      {
         System.clearProperty("existing");
      }
      
      if (oldNonExistingValue != null)
      {
         System.setProperty("non-existing", oldNonExistingValue);
      }
   }

   @Before
   public void before()
   {
      this.control = EasyMock.createControl();
      this.contentHandler = this.control.createMock(ContentHandler.class);
      this.dtdHandler = this.control.createMock(DTDHandler.class);
      this.errorHandler = this.control.createMock(ErrorHandler.class);
      this.resolver = this.control.createMock(EntityResolver.class);
      this.parent = this.control.createMock(XMLReader.class);
      
      this.filter = new PropertyReplacementFilter(this.parent);
      this.filter.setContentHandler(this.contentHandler);
      this.filter.setDTDHandler(this.dtdHandler);
      this.filter.setEntityResolver(this.resolver);
      this.filter.setErrorHandler(this.errorHandler);
   }
   
   @Test
   public void characters() throws SAXException
   {
      String plain = "test";
      
      this.contentHandler.characters(EasyMock.aryEq(plain.toCharArray()), EasyMock.eq(0), EasyMock.eq(plain.length()));
      
      this.control.replay();
      
      this.filter.characters(plain.toCharArray(), 0, plain.length());

      this.control.verify();
      this.control.reset();

      
      String string = "${dummy,existing:Free} the ${non-existing:Kraken}! ${dummy} ${/} ${:} ${} ${dummy:}";
      String expected = String.format("Release the Kraken! ${dummy} %s %s ${} ", Strings.FILE_SEPARATOR, Strings.PATH_SEPARATOR);
      
      this.contentHandler.characters(EasyMock.aryEq(expected.toCharArray()), EasyMock.eq(0), EasyMock.eq(expected.length()));
      
      this.control.replay();
      
      this.filter.characters(string.toCharArray(), 0, string.length());
      
      this.control.verify();
   }
   
   @Test
   public void startElement() throws SAXException
   {
      Attributes attributes = this.control.createMock(Attributes.class);
      Capture<Attributes> capturedAttributes = new Capture<Attributes>();
      
      String plain = "test";
      String attributeURI = "uri";
      String attributeLocalName = "localName";
      String attributeQName = "qName";
      String attributeType = "type";
      String elementURI = "uri";
      String elementLocalName = "localName";
      String elementQName = "qName";
      
      EasyMock.expect(attributes.getLength()).andReturn(1);
      EasyMock.expect(attributes.getURI(0)).andReturn(attributeURI);
      EasyMock.expect(attributes.getLocalName(0)).andReturn(attributeLocalName);
      EasyMock.expect(attributes.getQName(0)).andReturn(attributeQName);
      EasyMock.expect(attributes.getType(0)).andReturn(attributeType);
      EasyMock.expect(attributes.getValue(0)).andReturn(plain);
      
      this.contentHandler.startElement(EasyMock.eq(elementURI), EasyMock.eq(elementLocalName), EasyMock.eq(elementQName), EasyMock.capture(capturedAttributes));
      
      this.control.replay();

      this.filter.startElement(elementURI, elementLocalName, elementQName, attributes);
      
      this.control.verify();
      
      Attributes attr = capturedAttributes.getValue();
      Assert.assertEquals(1, attr.getLength());
      Assert.assertSame(attributeURI, attr.getURI(0));
      Assert.assertSame(attributeLocalName, attr.getLocalName(0));
      Assert.assertSame(attributeQName, attr.getQName(0));
      Assert.assertSame(attributeType, attr.getType(0));
      Assert.assertEquals(plain, attr.getValue(0));
      
      this.control.reset();
      
      
      String string = "${dummy,existing:Free} the ${non-existing:Kraken}! ${dummy} ${/} ${:} ${} ${dummy:}";
      String expected = String.format("Release the Kraken! ${dummy} %s %s ${} ", Strings.FILE_SEPARATOR, Strings.PATH_SEPARATOR);
      
      EasyMock.expect(attributes.getLength()).andReturn(1);
      EasyMock.expect(attributes.getURI(0)).andReturn(attributeURI);
      EasyMock.expect(attributes.getLocalName(0)).andReturn(attributeLocalName);
      EasyMock.expect(attributes.getQName(0)).andReturn(attributeQName);
      EasyMock.expect(attributes.getType(0)).andReturn(attributeType);
      EasyMock.expect(attributes.getValue(0)).andReturn(string);
      
      this.contentHandler.startElement(EasyMock.eq(elementURI), EasyMock.eq(elementLocalName), EasyMock.eq(elementQName), EasyMock.capture(capturedAttributes));
      
      this.control.replay();
      
      this.filter.startElement(elementURI, elementLocalName, elementQName, attributes);
      
      this.control.verify();
      
      attr = capturedAttributes.getValue();
      Assert.assertEquals(1, attr.getLength());
      Assert.assertSame(attributeURI, attr.getURI(0));
      Assert.assertSame(attributeLocalName, attr.getLocalName(0));
      Assert.assertSame(attributeQName, attr.getQName(0));
      Assert.assertSame(attributeType, attr.getType(0));
      Assert.assertEquals(expected, attr.getValue(0));
   }
   
   @Test
   public void getContentHandler()
   {
      Assert.assertSame(this.contentHandler, this.filter.getContentHandler());
   }
   
   @Test
   public void getDTDHandler()
   {
      Assert.assertSame(this.dtdHandler, this.filter.getDTDHandler());
   }
   
   @Test
   public void getEntityResolver()
   {
      Assert.assertSame(this.resolver, this.filter.getEntityResolver());
   }
   
   @Test
   public void getErrorHandler()
   {
      Assert.assertSame(this.errorHandler, this.filter.getErrorHandler());
   }
   
   @Test
   public void getFeature() throws SAXException
   {
      String feature = "name";
      
      EasyMock.expect(this.parent.getFeature(feature)).andReturn(true);
      
      this.control.replay();
      
      SAXException exception = null;
      Boolean result = null;
      try
      {
         result = this.filter.getFeature(feature);
      }
      catch (SAXException e)
      {
         exception = e;
      }
      
      this.control.verify();
      
      Assert.assertNull(exception);
      Assert.assertNotNull(result);
      Assert.assertTrue(result);
      
      this.control.reset();
      
      this.getFeature(feature, new SAXNotRecognizedException());
      this.getFeature(feature, new SAXNotSupportedException());
   }
   
   private void getFeature(String feature, SAXException expected) throws SAXException
   {
      EasyMock.expect(this.parent.getFeature(feature)).andThrow(expected);
      
      this.control.replay();
      
      SAXException exception = null;
      Boolean result = null;
      try
      {
         result = this.filter.getFeature(feature);
      }
      catch (SAXException e)
      {
         exception = e;
      }
      
      this.control.verify();
      
      Assert.assertNull(result);
      Assert.assertNotNull(exception);
      Assert.assertSame(expected, exception);
      
      this.control.reset();
   }
   
   @Test
   public void getParent()
   {
      this.control.replay();
      
      XMLReader result = this.filter.getParent();
      
      this.control.verify();
      
      Assert.assertSame(this.parent, result);
   }
   
   @Test
   public void getProperty() throws SAXException
   {
      String property = "name";
      Object expected = new Object();
      
      EasyMock.expect(this.parent.getProperty(property)).andReturn(expected);
      
      this.control.replay();
      
      Object result = this.filter.getProperty(property);
      
      this.control.verify();
      
      Assert.assertSame(expected, result);
      
      this.control.reset();
      
      this.getProperty(property, new SAXNotRecognizedException());
      this.getProperty(property, new SAXNotSupportedException());
   }
   
   private void getProperty(String property, SAXException expected) throws SAXException
   {
      EasyMock.expect(this.parent.getProperty(property)).andThrow(expected);
      
      this.control.replay();
      
      SAXException exception = null;
      
      try
      {
         this.filter.getProperty(property);
      }
      catch (SAXException e)
      {
         exception = e;
      }
      
      this.control.verify();
      
      Assert.assertNotNull(exception);
      Assert.assertSame(expected, exception);
      
      this.control.reset();
   }
   
   @Test
   public void parseInputSource() throws IOException, SAXException
   {
      InputSource source = EasyMock.createMock(InputSource.class);
      
      EasyMock.checkOrder(this.parent, false);
      this.parent.setContentHandler(this.filter);
      this.parent.setDTDHandler(this.filter);
      this.parent.setEntityResolver(this.filter);
      this.parent.setErrorHandler(this.filter);
      EasyMock.checkOrder(this.parent, true);
      
      this.parent.parse(source);
      
      this.control.replay();
      
      this.filter.parse(source);
      
      this.control.verify();
   }
   
   @Test
   public void parseSystemId() throws IOException, SAXException
   {
      Capture<InputSource> capturedSource = new Capture<InputSource>();
      String systemId = "";
      
      EasyMock.checkOrder(this.parent, false);
      this.parent.setContentHandler(this.filter);
      this.parent.setDTDHandler(this.filter);
      this.parent.setEntityResolver(this.filter);
      this.parent.setErrorHandler(this.filter);
      EasyMock.checkOrder(this.parent, true);
      
      this.parent.parse(EasyMock.capture(capturedSource));
      
      this.control.replay();
      
      this.filter.parse(systemId);
      
      this.control.verify();
      
      InputSource source = capturedSource.getValue();
      Assert.assertSame(systemId, source.getSystemId());
   }
   
   @Test
   public void setFeature() throws SAXException
   {
      String feature = "feature";
      boolean enabled = true;
      
      this.parent.setFeature(feature, enabled);
      
      this.control.replay();
      
      SAXException exception = null;
      try
      {
         this.filter.setFeature(feature, enabled);
      }
      catch (SAXException e)
      {
         exception = e;
      }
      
      this.control.verify();
      
      Assert.assertNull(exception);
      
      this.control.reset();
      
      this.setFeature(feature, enabled, new SAXNotRecognizedException());
      this.setFeature(feature, enabled, new SAXNotSupportedException());
   }
   
   private void setFeature(String feature, boolean enabled, SAXException expected) throws SAXException
   {
      this.parent.setFeature(feature, enabled);
      EasyMock.expectLastCall().andThrow(expected);
      
      this.control.replay();
      
      SAXException exception = null;
      try
      {
         this.filter.setFeature(feature, enabled);
      }
      catch (SAXException e)
      {
         exception = e;
      }
      
      this.control.verify();
      
      Assert.assertSame(expected, exception);
      
      this.control.reset();
   }
   
   @Test
   public void endDocument() throws SAXException
   {
      this.contentHandler.endDocument();
      
      this.control.replay();
      
      this.filter.endDocument();
      
      this.control.verify();
   }
   
   @Test
   public void endElement() throws SAXException
   {
      this.contentHandler.endElement("uri", "localName", "qName");
      
      this.control.replay();
      
      this.filter.endElement("uri", "localName", "qName");
      
      this.control.verify();
   }
   
   @Test
   public void endPrefixMapping() throws SAXException
   {
      this.contentHandler.endPrefixMapping("prefix");
      
      this.control.replay();
      
      this.filter.endPrefixMapping("prefix");
      
      this.control.verify();
   }
   
   @Test
   public void error() throws SAXException
   {
      SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
      
      this.errorHandler.error(error);
      
      this.control.replay();
      
      this.filter.error(error);
      
      this.control.verify();
   }
   
   @Test
   public void fatalError() throws SAXException
   {
      SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
      
      this.errorHandler.fatalError(error);
      
      this.control.replay();
      
      this.filter.fatalError(error);
      
      this.control.verify();
   }
   
   @Test
   public void ignorableWhitespace() throws SAXException
   {
      String space = " ";
      
      this.contentHandler.ignorableWhitespace(EasyMock.aryEq(space.toCharArray()), EasyMock.eq(0), EasyMock.eq(space.length()));
      
      this.control.replay();
      
      this.filter.ignorableWhitespace(space.toCharArray(), 0, space.length());
      
      this.control.verify();
   }
   
   @Test
   public void notationDecl() throws SAXException
   {
      this.dtdHandler.notationDecl("name", "publicId", "systemId");
      
      this.control.replay();
      
      this.filter.notationDecl("name", "publicId", "systemId");
      
      this.control.verify();
   }
   
   @Test
   public void processingInstruction() throws SAXException
   {
      this.contentHandler.processingInstruction("target", "data");
      
      this.control.replay();
      
      this.filter.processingInstruction("target", "data");
      
      this.control.verify();
   }
   
   @Test
   public void resolveEntity() throws SAXException, IOException
   {
      InputSource expected = EasyMock.createMock(InputSource.class);
      
      EasyMock.expect(this.resolver.resolveEntity("publicId", "systemId")).andReturn(expected);
      
      this.control.replay();
      
      InputSource result = this.filter.resolveEntity("publicId", "systemId");
      
      this.control.verify();
      
      Assert.assertSame(expected, result);
   }
   
   @Test
   public void skippedEntity() throws SAXException
   {
      this.contentHandler.skippedEntity("name");
      
      this.control.replay();
      
      this.filter.skippedEntity("name");
      
      this.control.verify();
   }
   
   @Test
   public void startDocument() throws SAXException
   {
      this.contentHandler.startDocument();
      
      this.control.replay();
      
      this.filter.startDocument();
      
      this.control.verify();
   }
   
   @Test
   public void startPrefixMapping() throws SAXException
   {
      this.contentHandler.startPrefixMapping("prefix", "uri");
      
      this.control.replay();
      
      this.filter.startPrefixMapping("prefix", "uri");
      
      this.control.verify();
   }
   
   @Test
   public void unparsedEntityDecl() throws SAXException
   {
      this.dtdHandler.unparsedEntityDecl("name", "publicId", "systemId", "notationName");
      
      this.control.replay();
      
      this.filter.unparsedEntityDecl("name", "publicId", "systemId", "notationName");
      
      this.control.verify();
   }
   
   @Test
   public void warning() throws SAXException
   {
      SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
      
      this.errorHandler.warning(error);
      
      this.control.replay();
      
      this.filter.warning(error);
      
      this.control.verify();
   }
}