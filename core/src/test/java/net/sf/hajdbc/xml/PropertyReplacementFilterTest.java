/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
import java.util.Properties;

import net.sf.hajdbc.util.Strings;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.*;

/**
 * @author Paul Ferraro
 */
public class PropertyReplacementFilterTest
{
	private ContentHandler contentHandler;
	private DTDHandler dtdHandler;
	private ErrorHandler errorHandler;
	private EntityResolver resolver;
	private XMLReader parent;
	private XMLFilterImpl filter;

	@Before
	public void before()
	{
		this.contentHandler = mock(ContentHandler.class);
		this.dtdHandler = mock(DTDHandler.class);
		this.errorHandler = mock(ErrorHandler.class);
		this.resolver = mock(EntityResolver.class);
		this.parent = mock(XMLReader.class);
		Properties properties = new Properties();
		properties.setProperty("existing", "Release");
		
		this.filter = new PropertyReplacementFilter(this.parent, properties);
		this.filter.setContentHandler(this.contentHandler);
		this.filter.setDTDHandler(this.dtdHandler);
		this.filter.setEntityResolver(this.resolver);
		this.filter.setErrorHandler(this.errorHandler);
	}
	
	@Test
	public void characters() throws SAXException
	{
		String plain = "test";
		
		this.filter.characters(plain.toCharArray(), 0, plain.length());
		
		verify(this.contentHandler).characters(aryEq(plain.toCharArray()), eq(0), eq(plain.length()));
		
		String string = "${dummy,existing:Free} the ${non-existing:Kraken}! ${dummy} ${/} ${:} ${} ${dummy:}";
		String expected = String.format("Release the Kraken! ${dummy} %s %s ${} ", Strings.FILE_SEPARATOR, Strings.PATH_SEPARATOR);
		
		this.filter.characters(string.toCharArray(), 0, string.length());
		
		verify(this.contentHandler).characters(aryEq(expected.toCharArray()), eq(0), eq(expected.length()));
	}
	
	@Test
	public void startElement() throws SAXException
	{
		Attributes attributes = mock(Attributes.class);
		ArgumentCaptor<Attributes> capturedAttributes = ArgumentCaptor.forClass(Attributes.class);
		
		String plain = "test";
		String attributeURI = "uri";
		String attributeLocalName = "localName";
		String attributeQName = "qName";
		String attributeType = "type";
		String elementURI = "uri";
		String elementLocalName = "localName";
		String elementQName = "qName";
		
		when(attributes.getLength()).thenReturn(1);
		when(attributes.getURI(0)).thenReturn(attributeURI);
		when(attributes.getLocalName(0)).thenReturn(attributeLocalName);
		when(attributes.getQName(0)).thenReturn(attributeQName);
		when(attributes.getType(0)).thenReturn(attributeType);
		when(attributes.getValue(0)).thenReturn(plain);
		
		doNothing().when(this.contentHandler).startElement(eq(elementURI), eq(elementLocalName), eq(elementQName), capturedAttributes.capture());
		
		this.filter.startElement(elementURI, elementLocalName, elementQName, attributes);
		
		Attributes attr = capturedAttributes.getValue();
		assertEquals(1, attr.getLength());
		assertSame(attributeURI, attr.getURI(0));
		assertSame(attributeLocalName, attr.getLocalName(0));
		assertSame(attributeQName, attr.getQName(0));
		assertSame(attributeType, attr.getType(0));
		assertEquals(plain, attr.getValue(0));
		
		
		String string = "${dummy,existing:Free} the ${non-existing:Kraken}! ${dummy} ${/} ${:} ${} ${dummy:}";
		String expected = String.format("Release the Kraken! ${dummy} %s %s ${} ", Strings.FILE_SEPARATOR, Strings.PATH_SEPARATOR);
		
		when(attributes.getLength()).thenReturn(1);
		when(attributes.getURI(0)).thenReturn(attributeURI);
		when(attributes.getLocalName(0)).thenReturn(attributeLocalName);
		when(attributes.getQName(0)).thenReturn(attributeQName);
		when(attributes.getType(0)).thenReturn(attributeType);
		when(attributes.getValue(0)).thenReturn(string);
		
		doNothing().when(this.contentHandler).startElement(eq(elementURI), eq(elementLocalName), eq(elementQName), capturedAttributes.capture());
		
		this.filter.startElement(elementURI, elementLocalName, elementQName, attributes);
		
		attr = capturedAttributes.getValue();
		assertEquals(1, attr.getLength());
		assertSame(attributeURI, attr.getURI(0));
		assertSame(attributeLocalName, attr.getLocalName(0));
		assertSame(attributeQName, attr.getQName(0));
		assertSame(attributeType, attr.getType(0));
		assertEquals(expected, attr.getValue(0));
	}
	
	@Test
	public void getContentHandler()
	{
		assertSame(this.contentHandler, this.filter.getContentHandler());
	}
	
	@Test
	public void getDTDHandler()
	{
		assertSame(this.dtdHandler, this.filter.getDTDHandler());
	}
	
	@Test
	public void getEntityResolver()
	{
		assertSame(this.resolver, this.filter.getEntityResolver());
	}
	
	@Test
	public void getErrorHandler()
	{
		assertSame(this.errorHandler, this.filter.getErrorHandler());
	}
	
	@Test
	public void getFeature() throws SAXException
	{
		String feature = "name";
		
		when(this.parent.getFeature(feature)).thenReturn(true);
		
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
		
		assertNull(exception);
		assertNotNull(result);
		assertTrue(result);
		
		this.getFeature(feature, new SAXNotRecognizedException());
		this.getFeature(feature, new SAXNotSupportedException());
	}
	
	private void getFeature(String feature, SAXException expected) throws SAXException
	{
		reset(this.parent);
		
		when(this.parent.getFeature(feature)).thenThrow(expected);
		
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
		
		assertNull(result);
		assertNotNull(exception);
		assertSame(expected, exception);
	}
	
	@Test
	public void getParent()
	{
		XMLReader result = this.filter.getParent();
		
		assertSame(this.parent, result);
	}
	
	@Test
	public void getProperty() throws SAXException
	{
		String property = "name";
		Object expected = new Object();
		
		when(this.parent.getProperty(property)).thenReturn(expected);
		
		Object result = this.filter.getProperty(property);
		
		assertSame(expected, result);
		
		this.getProperty(property, new SAXNotRecognizedException());
		this.getProperty(property, new SAXNotSupportedException());
	}
	
	private void getProperty(String property, SAXException expected) throws SAXException
	{
		reset(this.parent);
		
		when(this.parent.getProperty(property)).thenThrow(expected);
		
		SAXException exception = null;
		
		try
		{
			this.filter.getProperty(property);
		}
		catch (SAXException e)
		{
			exception = e;
		}
		
		assertNotNull(exception);
		assertSame(expected, exception);
	}
	
	@Test
	public void parseInputSource() throws IOException, SAXException
	{
		InputSource source = mock(InputSource.class);
		
//		checkOrder(this.parent, false);
//		EasyMock.checkOrder(this.parent, true);
		
		this.filter.parse(source);
		
		verify(this.parent).setContentHandler(this.filter);
		verify(this.parent).setDTDHandler(this.filter);
		verify(this.parent).setEntityResolver(this.filter);
		verify(this.parent).setErrorHandler(this.filter);
		verify(this.parent).parse(source);
	}
	
	@Test
	public void parseSystemId() throws IOException, SAXException
	{
		ArgumentCaptor<InputSource> capturedSource = ArgumentCaptor.forClass(InputSource.class);
		String systemId = "";
		
//		EasyMock.checkOrder(this.parent, false);
//		EasyMock.checkOrder(this.parent, true);
		
		this.filter.parse(systemId);

		verify(this.parent).setContentHandler(this.filter);
		verify(this.parent).setDTDHandler(this.filter);
		verify(this.parent).setEntityResolver(this.filter);
		verify(this.parent).setErrorHandler(this.filter);
		verify(this.parent).parse(capturedSource.capture());
		
		InputSource source = capturedSource.getValue();
		assertSame(systemId, source.getSystemId());
	}
	
	@Test
	public void setFeature() throws SAXException
	{
		String feature = "feature";
		boolean enabled = true;
		
		SAXException exception = null;
		try
		{
			this.filter.setFeature(feature, enabled);
		}
		catch (SAXException e)
		{
			exception = e;
		}
		
		verify(this.parent).setFeature(feature, enabled);
		
		assertNull(exception);
		
		this.setFeature(feature, enabled, new SAXNotRecognizedException());
		this.setFeature(feature, enabled, new SAXNotSupportedException());
	}
	
	private void setFeature(String feature, boolean enabled, SAXException expected) throws SAXException
	{
		doThrow(expected).when(this.parent).setFeature(feature, enabled);
		
		SAXException exception = null;
		try
		{
			this.filter.setFeature(feature, enabled);
		}
		catch (SAXException e)
		{
			exception = e;
		}
		
		assertSame(expected, exception);
	}
	
	@Test
	public void endDocument() throws SAXException
	{
		this.filter.endDocument();
		
		verify(this.contentHandler).endDocument();
	}
	
	@Test
	public void endElement() throws SAXException
	{
		this.filter.endElement("uri", "localName", "qName");
		
		verify(this.contentHandler).endElement("uri", "localName", "qName");
	}
	
	@Test
	public void endPrefixMapping() throws SAXException
	{
		this.filter.endPrefixMapping("prefix");
		
		verify(this.contentHandler).endPrefixMapping("prefix");
	}
	
	@Test
	public void error() throws SAXException
	{
		SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
		
		this.filter.error(error);
		
		verify(this.errorHandler).error(error);
	}
	
	@Test
	public void fatalError() throws SAXException
	{
		SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
		
		this.filter.fatalError(error);
		
		verify(this.errorHandler).fatalError(error);		
	}
	
	@Test
	public void ignorableWhitespace() throws SAXException
	{
		String space = " ";
		
		doNothing().when(this.contentHandler).ignorableWhitespace(aryEq(space.toCharArray()), eq(0), eq(space.length()));
		
		this.filter.ignorableWhitespace(space.toCharArray(), 0, space.length());
	}
	
	@Test
	public void notationDecl() throws SAXException
	{
		this.filter.notationDecl("name", "publicId", "systemId");
		
		verify(this.dtdHandler).notationDecl("name", "publicId", "systemId");
	}
	
	@Test
	public void processingInstruction() throws SAXException
	{
		this.filter.processingInstruction("target", "data");
		
		verify(this.contentHandler).processingInstruction("target", "data");
	}
	
	@Test
	public void resolveEntity() throws SAXException, IOException
	{
		InputSource expected = mock(InputSource.class);
		
		when(this.resolver.resolveEntity("publicId", "systemId")).thenReturn(expected);
		
		InputSource result = this.filter.resolveEntity("publicId", "systemId");
		
		assertSame(expected, result);
	}
	
	@Test
	public void skippedEntity() throws SAXException
	{
		this.filter.skippedEntity("name");
		
		verify(this.contentHandler).skippedEntity("name");
	}
	
	@Test
	public void startDocument() throws SAXException
	{
		this.filter.startDocument();
		
		verify(this.contentHandler).startDocument();
	}
	
	@Test
	public void startPrefixMapping() throws SAXException
	{
		this.filter.startPrefixMapping("prefix", "uri");
		
		verify(this.contentHandler).startPrefixMapping("prefix", "uri");
	}
	
	@Test
	public void unparsedEntityDecl() throws SAXException
	{
		this.filter.unparsedEntityDecl("name", "publicId", "systemId", "notationName");
		
		verify(this.dtdHandler).unparsedEntityDecl("name", "publicId", "systemId", "notationName");
	}
	
	@Test
	public void warning() throws SAXException
	{
		SAXParseException error = new SAXParseException("", "publicId", "systemId", 1, 2);
		
		this.filter.warning(error);
		
		verify(this.errorHandler).warning(error);
	}
}