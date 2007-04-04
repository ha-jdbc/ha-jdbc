/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 * @since 2.0
 */
public interface SQLProxy<D, E>
{
	public DatabaseCluster<D> getDatabaseCluster();
	
	public Map.Entry<Database<D>, E> entry();
	
	public Set<Map.Entry<Database<D>, E>> entries();
	
	public void addChild(SQLProxy<D, ?> child);

	public void removeChild(SQLProxy<D, ?> child);
	
	public void removeChildren();

	public SQLProxy<D, ?> getRoot();
	
	public E getObject(Database<D> database);
	
	public void retain(Set<Database<D>> databaseSet);
	
	public void record(Invoker<D, E, ?> invoker);
	
	public void handleFailures(SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException;
}
