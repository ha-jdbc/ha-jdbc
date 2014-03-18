package net.sf.hajdbc.xml;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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

public class DatabaseClusterConfigurationReader_3_0<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> implements DatabaseClusterConfigurationReader<Z, D, B>
{
	public static final DatabaseClusterConfigurationReaderFactory FACTORY = new DatabaseClusterConfigurationReaderFactory()
	{
		@Override
		public <Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> DatabaseClusterConfigurationReader<Z, D, B> createReader()
		{
			return new DatabaseClusterConfigurationReader_3_0<>();
		}
	};

	enum Element
	{
		DISTRIBUTABLE("distributable"),
		SYNC("sync"),
		STATE("state"),
		LOCK("lock"),
		CLUSTER("cluster"),
		;
		private final String name;
		
		private Element(String name)
		{
			this.name = name;
		}

		public String getLocalName()
		{
			return this.name;
		}

		private static final Map<String, Element> names = new HashMap<>();
		
		static
		{
			for (Element element: Element.values())
			{
				names.put(element.getLocalName(), element);
			}
		}
		
		public static Element forName(String name)
		{
			return names.get(name);
		}
	}

	enum ClusterAttribute
	{
		DEFAULT_SYNC("default-sync"),
		BALANCER("balancer"),
		META_DATA_CACHE("meta-data-cache"),
		DIALECT("dialect"),
		DURABILITY("durability"),
		INPUT_SINK("input-sink"),
		TRANSACTION_MODE("transaction-mode"),
		AUTO_ACTIVATE_SCHEDULE("auto-activate-schedule"),
		FAILURE_DETECT_SCHEDULE("failure-detect-schedule"),
		EVAL_CURRENT_DATE("eval-current-date"),
		EVAL_CURRENT_TIME("eval-current-time"),
		EVAL_CURRENT_TIMESTAMP("eval-current-timestamp"),
		EVAL_RAND("eval-rand"),
		DETECT_IDENTITY_COLUMNS("detect-identity-columns"),
		DETECT_SEQUENCES("detect-sequences"),
		ALLOW_EMPTY_CLUSTER("allow-empty-cluster"),
		;
		
		private final String name;

		private ClusterAttribute(String name)
		{
			this.name = name;
		}

		public String getLocalName()
		{
			return this.name;
		}

		private static final Map<String, ClusterAttribute> names = new HashMap<>();

		static
		{
			for (ClusterAttribute attribute: ClusterAttribute.values())
			{
				names.put(attribute.getLocalName(), attribute);
			}
		}
		
		public static ClusterAttribute forName(String name)
		{
			return names.get(name);
		}
	}

	private static final String ID = "id";
	private static final String PROPERTY = "property";
	private static final String NAME = "name";

	enum ClusterElement
	{
		DATABASE("database"),
		;
		
		private final String name;
		
		private ClusterElement(String name)
		{
			this.name = name;
		}

		public String getLocalName()
		{
			return this.name;
		}

		private static final Map<String, ClusterElement> names = new HashMap<>();

		static
		{
			for (ClusterElement element: ClusterElement.values())
			{
				names.put(element.getLocalName(), element);
			}
		}
		
		public static ClusterElement forName(String name)
		{
			return names.get(name);
		}
	}

	enum DatabaseElement
	{
		USER("user"),
		PASSWORD("password"),
		PROPERTY(DatabaseClusterConfigurationReader_3_0.PROPERTY),
		;
		
		private final String name;
		
		private DatabaseElement(String name)
		{
			this.name = name;
		}

		public String getLocalName()
		{
			return this.name;
		}

		private static final Map<String, DatabaseElement> names = new HashMap<>();

		static
		{
			for (DatabaseElement element: DatabaseElement.values())
			{
				names.put(element.getLocalName(), element);
			}
		}
		
		public static DatabaseElement forName(String name)
		{
			return names.get(name);
		}
	}

	enum DatabaseAttribute
	{
		ID(DatabaseClusterConfigurationReader_3_0.ID),
		LOCATION("location"),
		WEIGHT("weight"),
		LOCAL("local")
		;
		
		private final String name;
		
		private DatabaseAttribute(String name)
		{
			this.name = name;
		}

		public String getLocalName()
		{
			return this.name;
		}

		private static final Map<String, DatabaseAttribute> names = new HashMap<>();

		static
		{
			for (DatabaseAttribute attribute: DatabaseAttribute.values())
			{
				names.put(attribute.getLocalName(), attribute);
			}
		}

		public static DatabaseAttribute forName(String name)
		{
			return names.get(name);
		}
	}
	
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
