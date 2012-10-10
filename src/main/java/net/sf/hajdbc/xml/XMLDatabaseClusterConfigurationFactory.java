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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.text.MessageFormat;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXB;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.sax.SAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.SystemProperties;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * {@link DatabaseClusterConfigurationFactory} that parses an xml configuration file.
 * @author Paul Ferraro
 */
public class XMLDatabaseClusterConfigurationFactory<Z, D extends Database<Z>> implements DatabaseClusterConfigurationFactory<Z, D>
{
	private static final long serialVersionUID = -8796872297122349961L;
	
	private static final String CONFIG_PROPERTY_FORMAT = "ha-jdbc.{0}.configuration"; //$NON-NLS-1$
	private static final String CONFIG_PROPERTY = "ha-jdbc.configuration"; //$NON-NLS-1$
	private static final String DEFAULT_RESOURCE = "ha-jdbc-{0}.xml"; //$NON-NLS-1$
	
	private static final URL SCHEMA = findResource("ha-jdbc.xsd");

	private static final Logger logger = LoggerFactory.getLogger(XMLDatabaseClusterConfigurationFactory.class);
	
	private final Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass;
	private final XMLStreamFactory streamFactory;
	
	private static String identifyResource(String id)
	{
		String resource = SystemProperties.getSystemProperty(MessageFormat.format(CONFIG_PROPERTY_FORMAT, id));
		
		return (resource != null) ? resource : MessageFormat.format(SystemProperties.getSystemProperty(CONFIG_PROPERTY, DEFAULT_RESOURCE), id);
	}
	
	/**
	 * Algorithm for searching class loaders for HA-JDBC url.
	 * @param resource a resource name
	 * @return a URL for the HA-JDBC configuration resource
	 */
	private static URL findResource(String resource, ClassLoader loader)
	{
		try
		{
			return new URL(resource);
		}
		catch (MalformedURLException e)
		{
			return findResource(resource, loader, XMLDatabaseClusterConfigurationFactory.class.getClassLoader(), ClassLoader.getSystemClassLoader());
		}
	}

	private static URL findResource(String resource, ClassLoader... loaders)
	{
		if (loaders.length == 0) return findResource(resource, Thread.currentThread().getContextClassLoader());
		
		for (ClassLoader loader: loaders)
		{
			if (loader != null)
			{
				URL url = loader.getResource(resource);
				
				if (url != null) return url;
			}
		}
		throw new IllegalArgumentException(Messages.CONFIG_NOT_FOUND.getMessage(resource));
	}
	
	public XMLDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, String id, String resource)
	{
		this(targetClass, id, resource, Thread.currentThread().getContextClassLoader());
	}
	
	public XMLDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, String id, String resource, ClassLoader loader)
	{
		this(targetClass, findResource((resource == null) ? identifyResource(id) : MessageFormat.format(resource, id), loader));
	}
	
	public XMLDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, URL url)
	{
		this(targetClass, url.getProtocol().equals("file") ? new FileXMLStreamFactory(new File(url.getPath())) : new URLXMLStreamFactory(url));
	}
	
	public XMLDatabaseClusterConfigurationFactory(Class<? extends DatabaseClusterConfiguration<Z, D>> targetClass, XMLStreamFactory streamFactory)
	{
		this.targetClass = targetClass;
		this.streamFactory = streamFactory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationFactory#createConfiguration()
	 */
	@Override
	public DatabaseClusterConfiguration<Z, D> createConfiguration() throws SQLException
	{
		logger.log(Level.INFO, Messages.HA_JDBC_INIT.getMessage(), Version.getVersion(), this.streamFactory);
		
		try
		{
			SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = schemaFactory.newSchema(SCHEMA);
			
			Unmarshaller unmarshaller = JAXBContext.newInstance(this.targetClass).createUnmarshaller();
			unmarshaller.setSchema(schema);
			
			XMLReader reader = new PropertyReplacementFilter(XMLReaderFactory.createXMLReader());
			InputSource source = SAXSource.sourceToInputSource(this.streamFactory.createSource());

			return this.targetClass.cast(unmarshaller.unmarshal(new SAXSource(reader, source)));
		}
		catch (JAXBException e)
		{
			throw new SQLException(e);
		}
		catch (SAXException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationListener#added(net.sf.hajdbc.Database, net.sf.hajdbc.DatabaseClusterConfiguration)
	 */
	@Override
	public void added(D database, DatabaseClusterConfiguration<Z, D> configuration)
	{
		this.export(configuration);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfigurationListener#removed(net.sf.hajdbc.Database, net.sf.hajdbc.DatabaseClusterConfiguration)
	 */
	@Override
	public void removed(D database, DatabaseClusterConfiguration<Z, D> configuration)
	{
		this.export(configuration);
	}
	
	public void export(DatabaseClusterConfiguration<Z, D> configuration)
	{
		JAXB.marshal(configuration, this.streamFactory.createResult());
	}
}
