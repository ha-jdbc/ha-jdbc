/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.util.Locale;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.DatabaseBuilder;
import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.Locality;
import net.sf.hajdbc.configuration.ServiceBuilder;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;
import net.sf.hajdbc.sql.TransactionModeEnum;
import net.sf.hajdbc.util.ServiceLoaders;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("deprecation")
public class DatabaseClusterConfigurationReader_3_0<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> implements DatabaseClusterConfigurationReader<Z, D, B>, Constants
{
	private static Messages messages = MessagesFactory.getMessages();

	public static final DatabaseClusterConfigurationReaderFactory FACTORY = new DatabaseClusterConfigurationReaderFactory()
	{
		@Override
		public <Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> DatabaseClusterConfigurationReader<Z, D, B> createReader()
		{
			return new DatabaseClusterConfigurationReader_3_0<>();
		}
	};

	@Override
	public void read(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException
	{
		while (reader.nextTag() != END_ELEMENT)
		{
			switch (reader.getLocalName())
			{
				case DISTRIBUTABLE:
				{
					String id = requireAttributeValue(reader, ID, "jgroups");
					read(reader, builder.distributable(id));
					break;
				}
				case SYNC:
				{
					String id = requireAttributeValue(reader, ID);
					read(reader, builder.addSynchronizationStrategy(id));
					break;
				}
				case STATE:
				{
					String id = requireAttributeValue(reader, ID, "sql");
					read(reader, builder.state(id));
					break;
				}
				case LOCK:
				{
					String id = requireAttributeValue(reader, ID, "semaphore");
					read(reader, builder.lock(id));
					break;
				}
				case CLUSTER:
				{
					readCluster(reader, builder);
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedElement(reader));
				}
			}
		}
	}

	void readCluster(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException
	{
		this.readClusterAttributes(reader, builder);
		this.readClusterElements(reader, builder);
	}
	
	void readClusterAttributes(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException
	{
		for (int i = 0; i < reader.getAttributeCount(); ++i)
		{
			String value = reader.getAttributeValue(i);
			switch (reader.getAttributeLocalName(i))
			{
				case DEFAULT_SYNC:
				{
					builder.defaultSynchronizationStrategy(value);
					break;
				}
				case BALANCER:
				{
					builder.balancer(value);
					break;
				}
				case META_DATA_CACHE:
				{
					builder.metaDataCache(value);
					break;
				}
				case DIALECT:
				{
					builder.dialect(value);
					break;
				}
				case DURABILITY:
				{
					builder.durability(value);
					break;
				}
				case INPUT_SINK:
				{
					builder.inputSink(value);
					break;
				}
				case TRANSACTION_MODE:
				{
					builder.transactionMode(TransactionModeEnum.valueOf(value.toUpperCase(Locale.ENGLISH)));
					break;
				}
				case AUTO_ACTIVATE_SCHEDULE:
				{
					builder.autoActivateSchedule(value);
					break;
				}
				case FAILURE_DETECT_SCHEDULE:
				{
					builder.failureDetectSchedule(value);
					break;
				}
				case EVAL_CURRENT_DATE:
				{
					builder.evalCurrentDate(Boolean.parseBoolean(value));
					break;
				}
				case EVAL_CURRENT_TIME:
				{
					builder.evalCurrentTime(Boolean.parseBoolean(value));
					break;
				}
				case EVAL_CURRENT_TIMESTAMP:
				{
					builder.evalCurrentTimestamp(Boolean.parseBoolean(value));
					break;
				}
				case EVAL_RAND:
				{
					builder.evalRand(Boolean.parseBoolean(value));
					break;
				}
				case DETECT_IDENTITY_COLUMNS:
				{
					builder.detectIdentityColumns(Boolean.parseBoolean(value));
					break;
				}
				case DETECT_SEQUENCES:
				{
					builder.detectSequences(Boolean.parseBoolean(value));
					break;
				}
				case ALLOW_EMPTY_CLUSTER:
				{
					builder.allowEmptyCluster(Boolean.parseBoolean(value));
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedAttribute(reader, i));
				}
			}
		}
	}
	
	void readClusterElements(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException
	{
		while (reader.nextTag() != END_ELEMENT)
		{
			switch (reader.getLocalName())
			{
				case DATABASE:
				{
					String id = requireAttributeValue(reader, ID);
					this.readDatabase(reader, builder.addDatabase(id));
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedElement(reader));
				}
			}
		}
	}

	void readDatabase(XMLStreamReader reader, B builder) throws XMLStreamException
	{
		this.readDatabaseAttributes(reader, builder);
		this.readDatabaseElements(reader, builder);
	}
	
	void readDatabaseAttributes(XMLStreamReader reader, B builder) throws XMLStreamException
	{
		for (int i = 0; i < reader.getAttributeCount(); ++i)
		{
			String value = reader.getAttributeValue(i);
			switch (reader.getAttributeLocalName(i))
			{
				case ID:
				{
					break;
				}
				case LOCATION:
				{
					builder.location(value);
					break;
				}
				case WEIGHT:
				{
					builder.weight(Integer.parseInt(value));
					break;
				}
				case LOCAL:
				{
					builder.locality(Boolean.parseBoolean(value) ? Locality.LOCAL : Locality.REMOTE);
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedAttribute(reader, i));
				}
			}
		}
	}
	
	void readDatabaseElements(XMLStreamReader reader, B builder) throws XMLStreamException
	{
		String user = null;
		String password = null;
		while (reader.nextTag() != END_ELEMENT)
		{
			switch (reader.getLocalName())
			{
				case USER:
				{
					user = reader.getElementText();
					break;
				}
				case PASSWORD:
				{
					password = reader.getElementText();
					break;
				}
				case PROPERTY:
				{
					String name = requireAttributeValue(reader, NAME);
					builder.property(name, reader.getElementText());
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedElement(reader));
				}
			}
		}
		if (user != null)
		{
			builder.credentials(user, password);
		}
	}

	static <T extends Identifiable> T load(Class<T> serviceClass, String id)
	{
		return ServiceLoaders.findRequiredService(serviceClass, new IdentifiableMatcher<T>(id));
	}

	static <T extends Identifiable> void read(XMLStreamReader reader, ServiceBuilder<T> builder) throws XMLStreamException
	{
		while (reader.nextTag() != END_ELEMENT)
		{
			switch (reader.getLocalName())
			{
				case PROPERTY:
				{
					String name = requireAttributeValue(reader, NAME);
					builder.property(name, reader.getElementText());
					break;
				}
				default:
				{
					throw new XMLStreamException(messages.unexpectedElement(reader));
				}
			}
		}
	}

	static String requireAttributeValue(XMLStreamReader reader, String name) throws XMLStreamException
	{
		return requireAttributeValue(reader, name, null);
	}

	static String requireAttributeValue(XMLStreamReader reader, String name, String defaultValue) throws XMLStreamException
	{
		String value = reader.getAttributeValue(null, name);
		if (value == null)
		{
			if (defaultValue == null)
			{
				throw new XMLStreamException(messages.missingRequiredAttribute(reader, name));
			}
			return defaultValue;
		}
		return value;
	}
}
