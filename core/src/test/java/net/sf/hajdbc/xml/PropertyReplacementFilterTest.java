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

import java.util.Properties;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 */
public class PropertyReplacementFilterTest
{
	private final Properties properties = new Properties();
	private final XMLStreamReader reader = mock(XMLStreamReader.class);
	private final XMLStreamReader filter = new PropertyReplacementFilter(this.reader, this.properties);
	private final static String PROPERTY_NAME = "existing";
	private final static String SYSTEM_PROPERTY = String.format("${%s}", PROPERTY_NAME);
	private final static String PROPERTY_VALUE = "expected";
	
	@Before
	public void before()
	{
		this.properties.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
	}

	@Test
	public void close() throws XMLStreamException
	{
		this.filter.close();
		
		verify(this.reader).close();
	}

	@Test
	public void getAttributeCount()
	{
		int expected = 10;
		
		when(this.reader.getAttributeCount()).thenReturn(expected);

		int result = this.filter.getAttributeCount();
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributeLocalName()
	{
		String expected = "expected";
		int index = 10;
		
		when(this.reader.getAttributeLocalName(index)).thenReturn(expected);

		String result = this.filter.getAttributeLocalName(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributeName()
	{
		QName expected = QName.valueOf("expected");
		int index = 10;
		
		when(this.reader.getAttributeName(index)).thenReturn(expected);

		QName result = this.filter.getAttributeName(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributeNamespace()
	{
		String expected = "expected";
		int index = 10;
		
		when(this.reader.getAttributeNamespace(index)).thenReturn(expected);

		String result = this.filter.getAttributeNamespace(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributePrefix()
	{
		String expected = "expected";
		int index = 10;
		
		when(this.reader.getAttributePrefix(index)).thenReturn(expected);

		String result = this.filter.getAttributePrefix(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributeType()
	{
		String expected = "expected";
		int index = 10;
		
		when(this.reader.getAttributeType(index)).thenReturn(expected);

		String result = this.filter.getAttributeType(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getAttributeValue()
	{
		String expected = "expected";
		int index = 10;
		
		when(this.reader.getAttributeValue(index)).thenReturn(expected);

		String result = this.filter.getAttributeValue(index);
		
		assertSame(expected, result);
		
		when(this.reader.getAttributeValue(index)).thenReturn(SYSTEM_PROPERTY);
		
		result = this.filter.getAttributeValue(index);
		
		assertEquals(PROPERTY_VALUE, result);
	}

	@Test
	public void getAttributeValueByName()
	{
		String namespaceURI = "url";
		String localName = "name";
		String expected = "expected";
		
		when(this.reader.getAttributeValue(namespaceURI, localName)).thenReturn(expected);

		String result = this.filter.getAttributeValue(namespaceURI, localName);
		
		assertSame(expected, result);
		
		when(this.reader.getAttributeValue(namespaceURI, localName)).thenReturn(SYSTEM_PROPERTY);
		
		result = this.filter.getAttributeValue(namespaceURI, localName);
		
		assertEquals(PROPERTY_VALUE, result);
	}

	@Test
	public void getCharacterEncodingScheme()
	{
		String expected = "expected";

		when(this.reader.getCharacterEncodingScheme()).thenReturn(expected);
		
		String result = this.filter.getCharacterEncodingScheme();
		
		assertSame(expected, result);
	}

	@Test
	public void getElementText() throws XMLStreamException
	{
		String expected = "expected";
		
		when(this.reader.getElementText()).thenReturn(expected);

		String result = this.filter.getElementText();
		
		assertSame(expected, result);
		
		when(this.reader.getElementText()).thenReturn(String.format("Expect the un%s!", SYSTEM_PROPERTY));
		
		result = this.filter.getElementText();
		
		assertEquals("Expect the unexpected!", result);
	}

	@Test
	public void getEncoding()
	{
		String expected = "expected";

		when(this.reader.getEncoding()).thenReturn(expected);
		
		String result = this.filter.getEncoding();
		
		assertSame(expected, result);
	}

	@Test
	public void getEventType()
	{
		int expected = 10;

		when(this.reader.getEventType()).thenReturn(expected);
		
		int result = this.filter.getEventType();
		
		assertSame(expected, result);
	}

	@Test
	public void getLocalName()
	{
		String expected = "expected";

		when(this.reader.getLocalName()).thenReturn(expected);
		
		String result = this.filter.getLocalName();
		
		assertSame(expected, result);
	}

	@Test
	public void getLocation()
	{
		Location expected = mock(Location.class);

		when(this.reader.getLocation()).thenReturn(expected);
		
		Location result = this.filter.getLocation();
		
		assertSame(expected, result);
	}

	@Test
	public void getName()
	{
		QName expected = QName.valueOf("expected");

		when(this.reader.getName()).thenReturn(expected);
		
		QName result = this.filter.getName();
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespaceContext()
	{
		NamespaceContext expected = mock(NamespaceContext.class);

		when(this.reader.getNamespaceContext()).thenReturn(expected);
		
		NamespaceContext result = this.filter.getNamespaceContext();
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespaceCount()
	{
		int expected = 10;

		when(this.reader.getNamespaceCount()).thenReturn(expected);
		
		int result = this.filter.getNamespaceCount();
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespacePrefix()
	{
		String expected = "expected";
		int index = 10;

		when(this.reader.getNamespacePrefix(index)).thenReturn(expected);
		
		String result = this.filter.getNamespacePrefix(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespaceURI()
	{
		String expected = "expected";

		when(this.reader.getNamespaceURI()).thenReturn(expected);
		
		String result = this.filter.getNamespaceURI();
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespaceURIByPrefix()
	{
		String expected = "expected";
		String prefix = "prefix";

		when(this.reader.getNamespaceURI(prefix)).thenReturn(expected);
		
		String result = this.filter.getNamespaceURI(prefix);
		
		assertSame(expected, result);
	}

	@Test
	public void getNamespaceURIByIndex()
	{
		String expected = "expected";
		int index = 10;

		when(this.reader.getNamespaceURI(index)).thenReturn(expected);
		
		String result = this.filter.getNamespaceURI(index);
		
		assertSame(expected, result);
	}

	@Test
	public void getPIData()
	{
		String expected = "expected";
		
		when(this.reader.getPIData()).thenReturn(expected);

		String result = this.filter.getPIData();
		
		assertSame(expected, result);
	}

	@Test
	public void getPITarget()
	{
		String expected = "expected";
		
		when(this.reader.getPITarget()).thenReturn(expected);

		String result = this.filter.getPITarget();
		
		assertSame(expected, result);
	}

	@Test
	public void getPrefix()
	{
		String expected = "expected";
		
		when(this.reader.getPrefix()).thenReturn(expected);

		String result = this.filter.getPrefix();
		
		assertSame(expected, result);
	}

	@Test
	public void getProperty()
	{
		String property = "property";
		Object expected = new Object();
		
		when(this.reader.getProperty(property)).thenReturn(expected);
		
		Object result = this.filter.getProperty(property);
		
		assertSame(expected, result);
	}

	@Test
	public void getText()
	{
		String expected = "expected";
		
		when(this.reader.getText()).thenReturn(expected);

		String result = this.filter.getText();
		
		assertSame(expected, result);
		
		when(this.reader.getText()).thenReturn(String.format("Expect the un%s!", SYSTEM_PROPERTY));
		
		result = this.filter.getText();
		
		assertEquals("Expect the unexpected!", result);
	}

	@Test
	public void getTextCharacters()
	{
		char[] expected = "expected".toCharArray();
		
		when(this.reader.getTextCharacters()).thenReturn(expected);

		char[] result = this.filter.getTextCharacters();
		
		assertArrayEquals(expected, result);
		
		when(this.reader.getTextCharacters()).thenReturn(String.format("Expect the un%s!", SYSTEM_PROPERTY).toCharArray());
		
		result = this.filter.getTextCharacters();
		
		assertArrayEquals("Expect the unexpected!".toCharArray(), result);
	}

	@Test
	public void getTextLength()
	{
		int expected = 10;

		when(this.reader.getTextLength()).thenReturn(expected);
		
		int result = this.filter.getTextLength();
		
		assertSame(expected, result);
	}

	@Test
	public void getTextStart()
	{
		int expected = 10;

		when(this.reader.getTextStart()).thenReturn(expected);
		
		int result = this.filter.getTextStart();
		
		assertSame(expected, result);
	}

	@Test
	public void getVersion()
	{
		String expected = "expected";
		
		when(this.reader.getVersion()).thenReturn(expected);

		String result = this.filter.getVersion();
		
		assertSame(expected, result);
	}

	@Test
	public void hasName()
	{
		when(this.reader.hasName()).thenReturn(true);

		boolean result = this.filter.hasName();
		
		assertTrue(result);
	}

	@Test
	public void hasNext() throws XMLStreamException
	{
		when(this.reader.hasNext()).thenReturn(true);

		boolean result = this.filter.hasNext();
		
		assertTrue(result);
	}

	@Test
	public void hasText()
	{
		when(this.reader.hasText()).thenReturn(true);

		boolean result = this.filter.hasText();
		
		assertTrue(result);
	}

	@Test
	public void isAttributeSpecified()
	{
		int index = 10;
		
		when(this.reader.isAttributeSpecified(index)).thenReturn(true);

		boolean result = this.filter.isAttributeSpecified(index);
		
		assertTrue(result);
	}

	@Test
	public void isCharacters()
	{
		when(this.reader.isCharacters()).thenReturn(true);

		boolean result = this.filter.isCharacters();
		
		assertTrue(result);
	}

	@Test
	public void isEndElement()
	{
		when(this.reader.isEndElement()).thenReturn(true);

		boolean result = this.filter.isEndElement();
		
		assertTrue(result);
	}

	@Test
	public void isStandalone()
	{
		when(this.reader.isStandalone()).thenReturn(true);

		boolean result = this.filter.isStandalone();
		
		assertTrue(result);
	}

	@Test
	public void isStartElement()
	{
		when(this.reader.isStartElement()).thenReturn(true);

		boolean result = this.filter.isStartElement();
		
		assertTrue(result);
	}

	@Test
	public void isWhiteSpace()
	{
		when(this.reader.isWhiteSpace()).thenReturn(true);

		boolean result = this.filter.isWhiteSpace();
		
		assertTrue(result);
	}

	@Test
	public void next() throws XMLStreamException
	{
		int expected = 10;
		
		when(this.reader.next()).thenReturn(expected);

		int result = this.filter.next();
		
		assertEquals(expected, result);
	}

	@Test
	public void nextTag() throws XMLStreamException
	{
		int expected = 10;
		
		when(this.reader.nextTag()).thenReturn(expected);

		int result = this.filter.nextTag();
		
		assertEquals(expected, result);
	}

	@Test
	public void require() throws XMLStreamException
	{
		int type = 10;
		String uri = "uri";
		String name = "name";

		this.filter.require(type, uri, name);
		
		verify(this.reader).require(type, uri, name);
	}

	@Test
	public void standaloneSet()
	{
		when(this.reader.standaloneSet()).thenReturn(true);

		boolean result = this.filter.standaloneSet();
		
		assertTrue(result);
	}
}