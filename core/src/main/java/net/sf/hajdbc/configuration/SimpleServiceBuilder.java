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
package net.sf.hajdbc.configuration;

import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.util.ServiceLoaders;

public class SimpleServiceBuilder<T extends Identifiable> implements Builder<T>
{
	private volatile Class<T> serviceClass;
	private volatile String id;

	public SimpleServiceBuilder(Class<T> serviceClass, String id)
	{
		this.serviceClass = serviceClass;
		this.id = id;
	}
	
	@Override
	public SimpleServiceBuilder<T> read(T service)
	{
		this.id = service.getId();
		return this;
	}

	@Override
	public T build()
	{
		return ServiceLoaders.findRequiredService(new IdentifiableMatcher<T>(this.id), this.serviceClass);
	}
}
