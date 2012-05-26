/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
package net.sf.hajdbc;

import net.sf.hajdbc.cache.TableProperties;

/**
 * @author Paul Ferraro
 */
public interface TriggerSupport
{
	enum TriggerEventEnum implements TriggerEvent
	{
		INSERT(TriggerTimeEnum.AFTER, true), UPDATE(TriggerTimeEnum.AFTER, false), DELETE(TriggerTimeEnum.BEFORE, null);
		
		private final TriggerTime time;
		private final Boolean value;
		
		private TriggerEventEnum(TriggerTime time, Boolean value)
		{
			this.time = time;
			this.value = value;
		}

		@Override
		public TriggerTime getTime()
		{
			return this.time;
		}
		
		@Override
		public Boolean getValue()
		{
			return this.value;
		}
		
		static TriggerEvent valueOf(Boolean value)
		{
			for (TriggerEventEnum event: TriggerEventEnum.values())
			{
				if (((value != null) && (event.value != null)) ? value.equals(event.value) : (value == event.value))
				{
					return event;
				}
			}
			
			throw new IllegalArgumentException();
		}
	}
	
	enum TriggerTimeEnum implements TriggerTime
	{
		BEFORE("OLD"), AFTER("NEW");
		
		private final String alias;
		
		private TriggerTimeEnum(String alias)
		{
			this.alias = alias;
		}
		
		@Override
		public String getAlias()
		{
			return this.alias;
		}
	}
	
	String getTriggerRowAlias(TriggerTime time);
	
	String getCreateTriggerSQL(String name, TableProperties table, TriggerEvent event, String action);
	
	String getDropTriggerSQL(String name, TableProperties table);
}
