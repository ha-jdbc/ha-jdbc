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
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.Durability.Phase;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;
import net.sf.hajdbc.util.SimpleStaticRegistry;
import net.sf.hajdbc.util.SimpleStaticRegistry.ExceptionMessageFactory;
import net.sf.hajdbc.util.StaticRegistry;

/**
 * @author Paul Ferraro
 */
public class DurabilityPhaseRegistry implements StaticRegistry<Method, Durability.Phase>, ExceptionMessageFactory<Method>
{
	private static final Messages messages = MessagesFactory.getMessages();

	private final StaticRegistry<Method, Durability.Phase> registry;
	
	public DurabilityPhaseRegistry(List<Method> commitMethods, List<Method> rollbackMethods)
	{
		this(Collections.<Method>emptyList(), commitMethods, rollbackMethods, Collections.<Method>emptyList());
	}

	public DurabilityPhaseRegistry(List<Method> prepareMethods, List<Method> commitMethods, List<Method> rollbackMethods, List<Method> forgetMethods)
	{
		Map<Method, Durability.Phase> map = new HashMap<>();
		for (Method method: prepareMethods)
		{
			map.put(method, Durability.Phase.PREPARE);
		}
		for (Method method: commitMethods)
		{
			map.put(method, Durability.Phase.COMMIT);
		}
		for (Method method: rollbackMethods)
		{
			map.put(method, Durability.Phase.ROLLBACK);
		}
		for (Method method: forgetMethods)
		{
			map.put(method, Durability.Phase.FORGET);
		}
		this.registry = new SimpleStaticRegistry<>(this, map);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.StaticRegistry#get(java.lang.Object)
	 */
	@Override
	public Phase get(Method method)
	{
		return this.registry.get(method);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.SimpleStaticRegistry.ExceptionMessageFactory#createMessage(java.lang.Object)
	 */
	@Override
	public String createMessage(Method method)
	{
		return messages.noDurabilityPhase(method);
	}
}
