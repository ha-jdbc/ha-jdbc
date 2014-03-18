package net.sf.hajdbc.xml;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.DatabaseBuilder;

public interface DatabaseClusterConfigurationReader<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>>
{
	void read(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException;
}
