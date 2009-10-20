/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.durability;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.coarse.CoarseDurabilityFactory;
import net.sf.hajdbc.durability.fine.FineDurabilityFactory;
import net.sf.hajdbc.durability.none.NoDurabilityFactory;

/**
 * @author Paul Ferraro
 */
@XmlEnum(String.class)
@XmlType(name = "durability")
public enum DurabilityFactoryEnum implements DurabilityFactory
{
	@XmlEnumValue("fine")
	FINE(new FineDurabilityFactory()),
	@XmlEnumValue("coarse")
	COARSE(new CoarseDurabilityFactory()),
	@XmlEnumValue("none")
	NONE(new NoDurabilityFactory());
	
	private final DurabilityFactory factory;
	
	private DurabilityFactoryEnum(DurabilityFactory factory)
	{
		this.factory = factory;
	}

	@Override
	public <Z, D extends Database<Z>> Durability<Z, D> createDurability(DurabilityListener listener)
	{
		return this.factory.createDurability(listener);
	}
}
