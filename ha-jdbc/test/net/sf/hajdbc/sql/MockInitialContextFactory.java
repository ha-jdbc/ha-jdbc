/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.ObjectFactory;

public class MockInitialContextFactory implements InitialContextFactory
{
	private static ThreadLocal threadLocal = new ThreadLocal()
	{
		/**
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		protected Object initialValue()
		{
			return new MockContext();
		}	
	};
	
	public Context getInitialContext(Hashtable environment) throws NamingException
	{
		return (Context) threadLocal.get();
	}
	
	public static class MockContext implements Context
	{
		private Map referenceMap = new HashMap();
		
		public Object lookup(Name name) throws NamingException
		{
			return this.lookup(name.toString());
		}

		public Object lookup(String name) throws NamingException
		{
			Reference reference = (Reference) this.referenceMap.get(name.toString());
			
			if (reference == null)
			{
				throw new NameNotFoundException(name.toString());
			}
			
			try
			{
				ObjectFactory factory = (ObjectFactory) Thread.currentThread().getContextClassLoader().loadClass(reference.getFactoryClassName()).newInstance();
				
				return factory.getObjectInstance(reference, new CompositeName(name), this, null);
			}
			catch (Exception e)
			{
				NamingException exception = new NamingException();
				exception.setRootCause(e);
				exception.initCause(e);
				
				throw exception;
			}
		}

		public void bind(Name name, Object object) throws NamingException
		{
			this.bind(name.toString(), object);
		}

		public void bind(String name, Object object) throws NamingException
		{
			Reference reference = null;
			
			if (Reference.class.isInstance(object))
			{
				reference = (Reference) object;
			}
			else if (Referenceable.class.isInstance(object))
			{
				reference = ((Referenceable) object).getReference();
			}
			else
			{
				throw new NamingException("Must extend javax.naming.Reference or implement javax.naming.Referenceable");
			}
			
			this.referenceMap.put(name, reference);
		}

		public void rebind(Name name, Object object) throws NamingException
		{
			this.rebind(name.toString(), object);
		}

		public void rebind(String name, Object object) throws NamingException
		{
			this.bind(name, object);
		}

		public void unbind(Name name) throws NamingException
		{
			this.unbind(name.toString());
		}

		public void unbind(String name) throws NamingException
		{
			this.referenceMap.remove(name);
		}

		public void rename(Name oldName, Name newName) throws NamingException
		{
			this.rename(oldName.toString(), newName.toString());
		}

		public void rename(String oldName, String newName) throws NamingException
		{
			this.referenceMap.put(newName, this.referenceMap.remove(oldName));
		}

		public NamingEnumeration list(Name name) throws NamingException
		{
			return null;
		}

		public NamingEnumeration list(String name) throws NamingException
		{
			return null;
		}

		public NamingEnumeration listBindings(Name name) throws NamingException
		{
			return null;
		}

		public NamingEnumeration listBindings(String name) throws NamingException
		{
			return null;
		}

		public void destroySubcontext(Name name) throws NamingException
		{
		}

		public void destroySubcontext(String name) throws NamingException
		{
		}

		public Context createSubcontext(Name name) throws NamingException
		{
			return null;
		}

		public Context createSubcontext(String name) throws NamingException
		{
			return null;
		}

		public Object lookupLink(Name name) throws NamingException
		{
			return null;
		}

		public Object lookupLink(String name) throws NamingException
		{
			return null;
		}

		public NameParser getNameParser(Name name) throws NamingException
		{
			return null;
		}

		public NameParser getNameParser(String name) throws NamingException
		{
			return null;
		}

		public Name composeName(Name arg0, Name arg1) throws NamingException
		{
			return null;
		}

		public String composeName(String arg0, String arg1) throws NamingException
		{
			return null;
		}

		public Object addToEnvironment(String arg0, Object arg1) throws NamingException
		{
			return null;
		}

		public Object removeFromEnvironment(String arg0) throws NamingException
		{
			return null;
		}

		public Hashtable getEnvironment() throws NamingException
		{
			return null;
		}

		public void close() throws NamingException
		{
			this.referenceMap.clear();
		}

		public String getNameInNamespace() throws NamingException
		{
			return null;
		}
	}
}
