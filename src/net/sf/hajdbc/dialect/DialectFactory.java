/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.dialect;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Dialect;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DialectFactory
{
	private static Map<String, Class<? extends Dialect>> dialectMap = new HashMap<String, Class<? extends Dialect>>();
	
	static
	{
		dialectMap.put("default", DefaultDialect.class);
		dialectMap.put("db2", DB2Dialect.class);
		dialectMap.put("derby", DerbyDialect.class);
		dialectMap.put("firebird", FirebirdDialect.class);
		dialectMap.put("h2", HSQLDBDialect.class);
		dialectMap.put("hsqldb", HSQLDBDialect.class);
		dialectMap.put("ingres", IngresDialect.class);
		dialectMap.put("maxdb", MaxDBDialect.class);
		dialectMap.put("mckoi", MckoiDialect.class);
		dialectMap.put("mysql", MySQLDialect.class);
		dialectMap.put("oracle", MaxDBDialect.class);
		dialectMap.put("postgresql", PostgreSQLDialect.class);
	}
	
	/**
	 * Creates a new instance of the Dialect implementation from the specified class name.
	 * @param id the class name of a Dialect instance.
	 * @return a new Dialect instance
	 * @throws Exception if a Dialect instance could not be instantiated from the specified class name.
	 */
	public static Dialect deserialize(String id) throws Exception
	{
		Class<? extends Dialect> targetClass = (id != null) ? dialectMap.get(id.toLowerCase()) : DefaultDialect.class;
		
		if (targetClass == null)
		{
			targetClass = Class.forName(id).asSubclass(Dialect.class);
		}
		
		return targetClass.newInstance();
	}
	
	/**
	 * Return a String representation that identifies the specified Dialect.
	 * @param dialect a Dialect implementation
	 * @return the class name of this dialect
	 */
	public static String serialize(Dialect dialect)
	{
		return dialect.getClass().getName();
	}
	
	private DialectFactory()
	{
		// Hide constructor
	}
}
