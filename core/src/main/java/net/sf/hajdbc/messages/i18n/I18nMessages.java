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
package net.sf.hajdbc.messages.i18n;

import net.sf.hajdbc.messages.simple.SimpleMessages;

import org.xnap.commons.i18n.I18n;
import org.xnap.commons.i18n.I18nFactory;

public class I18nMessages extends SimpleMessages
{
	private final I18n i18n;

	public I18nMessages(String bundle)
	{
		this.i18n = I18nFactory.getI18n(SimpleMessages.class, bundle);
	}
	
	@Override
	protected String tr(String message)
	{
		return this.i18n.tr(message);
	}

	@Override
	protected String tr(String pattern, Object... args)
	{
		return this.i18n.tr(pattern, args);
	}
}
