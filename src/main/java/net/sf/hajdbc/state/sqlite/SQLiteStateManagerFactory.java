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
package net.sf.hajdbc.state.sqlite;

import java.io.File;
import java.text.MessageFormat;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.pool.generic.GenericObjectPoolConfiguration;
import net.sf.hajdbc.pool.generic.GenericObjectPoolFactory;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.util.Strings;

public class SQLiteStateManagerFactory extends GenericObjectPoolConfiguration implements StateManagerFactory
{
	private static final long serialVersionUID = 8990527398117188315L;

	private String locationPattern = "{1}/{0}";

	@Override
	public String getId()
	{
		return "sqlite";
	}

	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster)
	{
		String location = MessageFormat.format(this.locationPattern, cluster.getId(), Strings.HA_JDBC_HOME);
		return new SQLiteStateManager<Z, D>(cluster, new File(location), new GenericObjectPoolFactory(this));
	}

	public String getLocationPattern()
	{
		return this.locationPattern;
	}

	public void setLocationPattern(String pattern)
	{
		this.locationPattern = pattern;
	}
}
