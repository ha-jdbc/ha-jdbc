package net.sf.hajdbc.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.sf.hajdbc.util.Strings;

public class FormattedXMLStreamWriter implements XMLStreamWriter
{
	private final XMLStreamWriter writer;
	private int indent = 0;
	private boolean indentEndElement = true;
	
	public FormattedXMLStreamWriter(XMLStreamWriter writer)
	{
		this.writer = writer;
	}

	private void indent() throws XMLStreamException
	{
		this.writer.writeCharacters(Strings.NEW_LINE);
		for (int i = 0; i < this.indent; ++i)
		{
			this.writer.writeCharacters(Strings.TAB);
		}
	}

	@Override
	public void writeStartElement(String localName) throws XMLStreamException
	{
		this.indent();
		this.writer.writeStartElement(localName);
		this.indent += 1;
	}

	@Override
	public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException
	{
		this.indent();
		this.writer.writeStartElement(namespaceURI, localName);
		this.indent += 1;
	}

	@Override
	public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException
	{
		this.indent();
		this.writer.writeStartElement(prefix, namespaceURI, localName);
		this.indent += 1;
	}

	@Override
	public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException
	{
		this.indent();
		this.writer.writeEmptyElement(namespaceURI, localName);
	}

	@Override
	public void writeEmptyElement(String prefix, String localName, String namespaceURI) throws XMLStreamException
	{
		this.indent();
		this.writer.writeEmptyElement(prefix, namespaceURI, localName);
	}

	@Override
	public void writeEmptyElement(String localName) throws XMLStreamException
	{
		this.indent();
		this.writer.writeEmptyElement(localName);
	}

	@Override
	public void writeEndElement() throws XMLStreamException
	{
		this.indent -= 1;
		if (this.indentEndElement)
		{
			this.indent();
		}
		else
		{
			this.indentEndElement = true;
		}
		this.writer.writeEndElement();
	}

	@Override
	public void writeEndDocument() throws XMLStreamException
	{
		this.writer.writeEndDocument();
	}

	@Override
	public void close() throws XMLStreamException
	{
		this.writer.close();
	}

	@Override
	public void flush() throws XMLStreamException
	{
		this.writer.flush();
	}

	@Override
	public void writeAttribute(String localName, String value) throws XMLStreamException
	{
		this.writer.writeAttribute(localName, value);
	}

	@Override
	public void writeAttribute(String prefix, String namespaceURI, String localName, String value) throws XMLStreamException
	{
		this.writer.writeAttribute(prefix, namespaceURI, localName, value);
	}

	@Override
	public void writeAttribute(String namespaceURI, String localName, String value) throws XMLStreamException
	{
		this.writer.writeAttribute(namespaceURI, localName, value);
	}

	@Override
	public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException
	{
		this.writer.writeNamespace(prefix, namespaceURI);
	}

	@Override
	public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException
	{
		this.writer.writeDefaultNamespace(namespaceURI);
	}

	@Override
	public void writeComment(String data) throws XMLStreamException
	{
		this.indent();
		this.writer.writeComment(data);
	}

	@Override
	public void writeProcessingInstruction(String target) throws XMLStreamException
	{
		this.writer.writeProcessingInstruction(target);
	}

	@Override
	public void writeProcessingInstruction(String target, String data) throws XMLStreamException
	{
		this.writer.writeProcessingInstruction(target, data);
	}

	@Override
	public void writeCData(String data) throws XMLStreamException
	{
		this.writer.writeCData(data);
		this.indentEndElement = false;
	}

	@Override
	public void writeDTD(String dtd) throws XMLStreamException
	{
		this.writer.writeDTD(dtd);
	}

	@Override
	public void writeEntityRef(String name) throws XMLStreamException
	{
		this.writer.writeEntityRef(name);
	}

	@Override
	public void writeStartDocument() throws XMLStreamException
	{
		this.writer.writeStartDocument();
	}

	@Override
	public void writeStartDocument(String version) throws XMLStreamException
	{
		this.writer.writeStartDocument(version);
	}

	@Override
	public void writeStartDocument(String encoding, String version) throws XMLStreamException
	{
		this.writer.writeStartDocument(encoding, version);
	}

	@Override
	public void writeCharacters(String text) throws XMLStreamException
	{
		this.writer.writeCharacters(text);
		this.indentEndElement = false;
	}

	@Override
	public void writeCharacters(char[] text, int start, int len) throws XMLStreamException
	{
		this.writer.writeCharacters(text, start, len);
		this.indentEndElement = false;
	}

	@Override
	public String getPrefix(String uri) throws XMLStreamException
	{
		return this.writer.getPrefix(uri);
	}

	@Override
	public void setPrefix(String prefix, String uri) throws XMLStreamException
	{
		this.writer.setPrefix(prefix, uri);
	}

	@Override
	public void setDefaultNamespace(String uri) throws XMLStreamException
	{
		this.writer.setDefaultNamespace(uri);
	}

	@Override
	public void setNamespaceContext(NamespaceContext context) throws XMLStreamException
	{
		this.writer.setNamespaceContext(context);
	}

	@Override
	public NamespaceContext getNamespaceContext()
	{
		return this.writer.getNamespaceContext();
	}

	@Override
	public Object getProperty(String name) throws IllegalArgumentException
	{
		return this.writer.getProperty(name);
	}
}
