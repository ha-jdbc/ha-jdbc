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
import net.sf.hajdbc.configuration.ServiceBuilder;
import net.sf.hajdbc.sql.TransactionModeEnum;
import net.sf.hajdbc.util.ServiceLoaders;

public class DatabaseClusterConfigurationReader_3_0<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> implements DatabaseClusterConfigurationReader<Z, D, B>, Constants
{
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
			Element element = Element.forName(reader.getLocalName());
			if (element == null)
			{
				throw new XMLStreamException();
			}
			switch (element)
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
			}
		}
	}

	void readCluster(XMLStreamReader reader, DatabaseClusterConfigurationBuilder<Z, D, B> builder) throws XMLStreamException
	{
		requireAttributeValue(reader, ClusterAttribute.DEFAULT_SYNC.getLocalName());
		for (int i = 0; i < reader.getAttributeCount(); ++i)
		{
			ClusterAttribute attribute = ClusterAttribute.forName(reader.getAttributeLocalName(i));
			if (attribute == null)
			{
				throw new XMLStreamException();
			}
			String value = reader.getAttributeValue(i);
			switch (attribute)
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
			}
		}
		while (reader.nextTag() != END_ELEMENT)
		{
			ClusterElement element = ClusterElement.forName(reader.getLocalName());
			if (element == null)
			{
				throw new XMLStreamException();
			}
			switch (element)
			{
				case DATABASE:
				{
					String id = requireAttributeValue(reader, ID);
					this.readDatabase(reader, builder.addDatabase(id));
					break;
				}
			}
		}
	}

	void readDatabase(XMLStreamReader reader, B builder) throws XMLStreamException
	{
		builder.location(requireAttributeValue(reader, DatabaseAttribute.LOCATION.getLocalName()));

		for (int i = 0; i < reader.getAttributeCount(); ++i)
		{
			DatabaseAttribute attribute = DatabaseAttribute.forName(reader.getAttributeLocalName(i));
			if (attribute == null)
			{
				throw new XMLStreamException();
			}
			String value = reader.getAttributeValue(i);
			switch (attribute)
			{
				case WEIGHT:
				{
					builder.weight(Integer.parseInt(value));
					break;
				}
				case LOCAL:
				{
					builder.local(Boolean.parseBoolean(value));
					break;
				}
				case ID:
				case LOCATION:
				{
					// Already read
					break;
				}
			}
		}
		String user = null;
		String password = null;
		while (reader.nextTag() != END_ELEMENT)
		{
			DatabaseElement element = DatabaseElement.forName(reader.getLocalName());
			if (element == null)
			{
				throw new XMLStreamException();
			}
			switch (element)
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
			}
		}
		if (user != null)
		{
			builder.credentials(user, password);
		}
	}

	static <T extends Identifiable> T load(Class<T> serviceClass, String id)
	{
		return ServiceLoaders.findRequiredService(new IdentifiableMatcher<T>(id), serviceClass);
	}

	static <T extends Identifiable> void read(XMLStreamReader reader, ServiceBuilder<T> builder) throws XMLStreamException
	{
		while (reader.nextTag() != END_ELEMENT)
		{
			verifyElement(reader, PROPERTY);
			String name = requireAttributeValue(reader, NAME);
			builder.property(name, reader.getElementText());
		}
	}

	static void verifyElement(XMLStreamReader reader, String name)
	{
		if (!reader.getLocalName().equals(name))
		{
			throw new IllegalArgumentException();
		}
	}

	static String requireAttributeValue(XMLStreamReader reader, String name)
	{
		return requireAttributeValue(reader, name, null);
	}

	static String requireAttributeValue(XMLStreamReader reader, String name, String defaultValue)
	{
		String value = reader.getAttributeValue(null, name);
		if (value == null)
		{
			if (defaultValue == null)
			{
				throw new IllegalArgumentException();
			}
			return defaultValue;
		}
		return value;
	}
}
