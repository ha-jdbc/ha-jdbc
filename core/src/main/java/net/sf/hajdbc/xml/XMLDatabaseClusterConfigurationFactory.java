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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.SystemProperties;

/**
 * {@link DatabaseClusterConfigurationFactory} that parses an xml configuration file.
 * @author Paul Ferraro
 */
public class XMLDatabaseClusterConfigurationFactory<Z, D extends Database<Z>> implements DatabaseClusterConfigurationFactory<Z, D>, DatabaseClusterConfigurationWriter<Z, D>, Constants
{
	private static final long serialVersionUID = -8796872297122349961L;
	
	private static final String CONFIG_PROPERTY_FORMAT = "ha-jdbc.{0}.configuration";
	private static final String CONFIG_PROPERTY = "ha-jdbc.configuration";
	private static final String DEFAULT_RESOURCE = "ha-jdbc-{0}.xml";

	private static final Logger logger = LoggerFactory.getLogger(XMLDatabaseClusterConfigurationFactory.class);
	
	private final XMLStreamFactory streamFactory;
	private final Map<String, Namespace> namespaces = new HashMap<>();

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

	public XMLDatabaseClusterConfigurationFactory(String id, String resource)
	{
		this(id, resource, XMLDatabaseClusterConfigurationFactory.class.getClassLoader());
	}
	
	public XMLDatabaseClusterConfigurationFactory(String id, String resource, ClassLoader loader)
	{
		this(findResource((resource == null) ? identifyResource(id) : MessageFormat.format(resource, id), loader));
	}
	
	public XMLDatabaseClusterConfigurationFactory(URL url)
	{
		this(url.getProtocol().equals("file") ? new FileXMLStreamFactory(url) : new URLXMLStreamFactory(url));
	}
	
	public XMLDatabaseClusterConfigurationFactory(XMLStreamFactory streamFactory)
	{
		this.streamFactory = streamFactory;
		for (Namespace namespace: Namespace.values())
		{
			this.namespaces.put(namespace.getURI(), namespace);
		}
	}
	
	@Override
	public <B extends DatabaseBuilder<Z, D>> DatabaseClusterConfiguration<Z, D> createConfiguration(DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws SQLException
	{
		logger.log(Level.INFO, Messages.HA_JDBC_INIT.getMessage(), Version.CURRENT, this.streamFactory);
		
		try
		{
			XMLStreamReader reader = new PropertyReplacementFilter(XMLInputFactory.newFactory().createXMLStreamReader(this.streamFactory.createSource()));
			reader.nextTag();
			String uri = reader.getNamespaceURI();
			Namespace namespace = this.namespaces.get(uri);
			
			if (namespace == null)
			{
				throw new SQLException("Unsupported namespace: " + uri);
			}
			
			namespace.getReaderFactory().<Z, D, B>createReader().read(reader, builder);
			return builder.build();
		}
		catch (XMLStreamException e)
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
//		JAXB.marshal(configuration, this.streamFactory.createResult());
	}

	@Override
	public void write(XMLStreamWriter writer, DatabaseClusterConfiguration<Z, D> config) throws XMLStreamException
	{
		writer.writeStartDocument();
		writer.setDefaultNamespace(Namespace.CURRENT_VERSION.getURI());
		writer.writeStartElement(ROOT);
		write(writer, Element.DISTRIBUTABLE, config.getDispatcherFactory());
		for (SynchronizationStrategy strategy: config.getSynchronizationStrategyMap().values())
		{
			write(writer, Element.SYNC, strategy);
		}
		write(writer, Element.STATE, config.getStateManagerFactory());
		write(writer, Element.LOCK, config.getLockManagerFactory());
		writer.writeStartElement(Element.CLUSTER.getLocalName());
		writeAttribute(writer, ClusterAttribute.ALLOW_EMPTY_CLUSTER, config.isEmptyClusterAllowed());
		writeAttribute(writer, ClusterAttribute.AUTO_ACTIVATE_SCHEDULE, config.getAutoActivationExpression());
		writeAttribute(writer, ClusterAttribute.BALANCER, config.getBalancerFactory());
		writeAttribute(writer, ClusterAttribute.DEFAULT_SYNC, config.getDefaultSynchronizationStrategy());
		writeAttribute(writer, ClusterAttribute.DETECT_IDENTITY_COLUMNS, config.isIdentityColumnDetectionEnabled());
		writeAttribute(writer, ClusterAttribute.DETECT_SEQUENCES, config.isSequenceDetectionEnabled());
		writeAttribute(writer, ClusterAttribute.DIALECT, config.getDialectFactory());
		writeAttribute(writer, ClusterAttribute.DURABILITY, config.getDurabilityFactory());
		writeAttribute(writer, ClusterAttribute.EVAL_CURRENT_DATE, config.isCurrentDateEvaluationEnabled());
		writeAttribute(writer, ClusterAttribute.EVAL_CURRENT_TIME, config.isCurrentTimeEvaluationEnabled());
		writeAttribute(writer, ClusterAttribute.EVAL_CURRENT_TIMESTAMP, config.isCurrentTimestampEvaluationEnabled());
		writeAttribute(writer, ClusterAttribute.EVAL_RAND, config.isRandEvaluationEnabled());
		writeAttribute(writer, ClusterAttribute.FAILURE_DETECT_SCHEDULE, config.getFailureDetectionExpression());
		writeAttribute(writer, ClusterAttribute.INPUT_SINK, config.getInputSinkProvider());
		writeAttribute(writer, ClusterAttribute.META_DATA_CACHE, config.getDatabaseMetaDataCacheFactory());
		writeAttribute(writer, ClusterAttribute.TRANSACTION_MODE, config.getTransactionMode());
		for (D database: config.getDatabaseMap().values())
		{
			writer.writeStartElement(ClusterElement.DATABASE.getLocalName());
			writer.writeAttribute(DatabaseAttribute.ID.getLocalName(), database.getId());
			writeAttribute(writer, DatabaseAttribute.LOCAL, database.isLocal());
			writeAttribute(writer, DatabaseAttribute.WEIGHT, Integer.valueOf(database.getWeight()));
			Credentials credentials = database.getCredentials();
			if (credentials != null)
			{
				writer.writeStartElement(DatabaseElement.USER.getLocalName());
				writer.writeCharacters(credentials.getUser());
				writer.writeEndElement();
				writer.writeStartElement(DatabaseElement.PASSWORD.getLocalName());
				writer.writeCharacters(credentials.getEncodedPassword());
				writer.writeEndElement();
			}
			writer.writeEndElement();
		}
		writer.writeEndElement();
		writer.writeEndElement();
		writer.writeEndDocument();
	}

	private static void writeAttribute(XMLStreamWriter writer, Named named, Object value) throws XMLStreamException
	{
		if (value != null)
		{
			writer.writeAttribute(named.getLocalName(), value.toString());
		}
	}

	private static void writeAttribute(XMLStreamWriter writer, Named named, Identifiable value) throws XMLStreamException
	{
		if (value != null)
		{
			writer.writeAttribute(named.getLocalName(), value.getId());
		}
	}

	private static void writeAttribute(XMLStreamWriter writer, Named named, boolean value) throws XMLStreamException
	{
		writer.writeAttribute(named.getLocalName(), String.valueOf(value));
	}

	private static void write(XMLStreamWriter writer, Named named, Identifiable object) throws XMLStreamException
	{
		if (object != null)
		{
			writer.writeStartElement(named.getLocalName());
			writer.writeAttribute(ID, object.getId());
			try
			{
				for (PropertyDescriptor descriptor: Introspector.getBeanInfo(object.getClass()).getPropertyDescriptors())
				{
					Method readMethod = descriptor.getReadMethod();
					if ((readMethod != null) && (descriptor.getWriteMethod() != null))
					{
						Object value = readMethod.invoke(object);
						if (value != null)
						{
							PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
							if (editor != null)
							{
								editor.setValue(value);
								writer.writeStartElement(PROPERTY);
								writer.writeAttribute(NAME, descriptor.getName());
								writer.writeCharacters(editor.getAsText());
								writer.writeEndElement();
							}
						}
					}
				}
			}
			catch (IllegalAccessException | InvocationTargetException | IntrospectionException e)
			{
				throw new XMLStreamException(e);
			}
			writer.writeEndElement();
		}
	}
}
