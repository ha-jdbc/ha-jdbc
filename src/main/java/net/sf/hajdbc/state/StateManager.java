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
package net.sf.hajdbc.state;

import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.DatabaseClusterListener;
import net.sf.hajdbc.Lifecycle;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;


/**
 * @author Paul Ferraro
 */
public interface StateManager extends DatabaseClusterListener, DurabilityListener, Lifecycle
{
	public static final String CLEAR_LOCAL_STATE = "ha-jdbc.state.clear";
	
	Set<String> getActiveDatabases();
	
	void setActiveDatabases(Set<String> databases);
	
	Map<InvocationEvent, Map<String, InvokerEvent>> recover();
}
