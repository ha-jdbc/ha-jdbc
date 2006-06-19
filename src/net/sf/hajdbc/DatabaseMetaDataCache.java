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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.cache.ColumnProperties;


/**
 * @author Paul Ferraro
 * @since 1.2
 */
public interface DatabaseMetaDataCache
{
	public void flush() throws SQLException;
	
	public void setConnection(Connection connection);
	
	public Map<String, Collection<String>> getTables() throws SQLException;
	
	public UniqueConstraint getPrimaryKey(String schema, String table) throws SQLException;
	
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(String schema, String table) throws SQLException;

	public Collection<UniqueConstraint> getUniqueConstraints(String schema, String table) throws SQLException;
	
	public Map<String, ColumnProperties> getColumns(String schema, String table) throws SQLException;
	
	public String getQualifiedTableForDDL(String schema, String table) throws SQLException;

	public String getQualifiedTableForDML(String schema, String table) throws SQLException;

	public boolean supportsSelectForUpdate() throws SQLException;
	
	public boolean containsAutoIncrementColumn(String qualifiedTable) throws SQLException;
}