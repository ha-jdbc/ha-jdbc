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

import java.util.HashMap;
import java.util.Map;

/**
 * XML constants
 * @author Paul Ferraro
 */
public interface Constants
{
	final String ROOT = "ha-jdbc";
	final String ID = "id";
	final String PROPERTY = "property";
	final String NAME = "name";

	interface Named
	{
		String getLocalName();
	}

	enum Element implements Named
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

		@Override
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

	enum ClusterAttribute implements Named
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

		@Override
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

	enum ClusterElement implements Named
	{
		DATABASE("database"),
		;
		
		private final String name;
		
		private ClusterElement(String name)
		{
			this.name = name;
		}

		@Override
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

	enum DatabaseElement implements Named
	{
		USER("user"),
		PASSWORD("password"),
		PROPERTY(Constants.PROPERTY),
		;
		
		private final String name;
		
		private DatabaseElement(String name)
		{
			this.name = name;
		}

		@Override
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

	enum DatabaseAttribute implements Named
	{
		ID(Constants.ID),
		LOCATION("location"),
		WEIGHT("weight"),
		LOCAL("local")
		;
		
		private final String name;
		
		private DatabaseAttribute(String name)
		{
			this.name = name;
		}

		@Override
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
}
