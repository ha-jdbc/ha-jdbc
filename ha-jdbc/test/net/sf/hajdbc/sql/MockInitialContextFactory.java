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

/**
 * Mock initial context factory that creates mock contexts.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
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
	
	/**
	 * @see javax.naming.spi.InitialContextFactory#getInitialContext(java.util.Hashtable)
	 */
	public Context getInitialContext(Hashtable environment) throws NamingException
	{
		return (Context) threadLocal.get();
	}
	
	/**
	 * @author  Paul Ferraro
	 * @since   1.1
	 */
	public static class MockContext implements Context
	{
		private Map referenceMap = new HashMap();
		
		/**
		 * @see javax.naming.Context#lookup(javax.naming.Name)
		 */
		public Object lookup(Name name) throws NamingException
		{
			return this.lookup(name.toString());
		}

		/**
		 * @see javax.naming.Context#lookup(java.lang.String)
		 */
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

		/**
		 * @see javax.naming.Context#bind(javax.naming.Name, java.lang.Object)
		 */
		public void bind(Name name, Object object) throws NamingException
		{
			this.bind(name.toString(), object);
		}

		/**
		 * @see javax.naming.Context#bind(java.lang.String, java.lang.Object)
		 */
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

		/**
		 * @see javax.naming.Context#rebind(javax.naming.Name, java.lang.Object)
		 */
		public void rebind(Name name, Object object) throws NamingException
		{
			this.rebind(name.toString(), object);
		}

		/**
		 * @see javax.naming.Context#rebind(java.lang.String, java.lang.Object)
		 */
		public void rebind(String name, Object object) throws NamingException
		{
			this.bind(name, object);
		}

		/**
		 * @see javax.naming.Context#unbind(javax.naming.Name)
		 */
		public void unbind(Name name) throws NamingException
		{
			this.unbind(name.toString());
		}

		/**
		 * @see javax.naming.Context#unbind(java.lang.String)
		 */
		public void unbind(String name) throws NamingException
		{
			this.referenceMap.remove(name);
		}

		/**
		 * @see javax.naming.Context#rename(javax.naming.Name, javax.naming.Name)
		 */
		public void rename(Name oldName, Name newName) throws NamingException
		{
			this.rename(oldName.toString(), newName.toString());
		}

		/**
		 * @see javax.naming.Context#rename(java.lang.String, java.lang.String)
		 */
		public void rename(String oldName, String newName) throws NamingException
		{
			this.referenceMap.put(newName, this.referenceMap.remove(oldName));
		}

		/**
		 * @see javax.naming.Context#list(javax.naming.Name)
		 */
		public NamingEnumeration list(Name name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#list(java.lang.String)
		 */
		public NamingEnumeration list(String name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#listBindings(javax.naming.Name)
		 */
		public NamingEnumeration listBindings(Name name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#listBindings(java.lang.String)
		 */
		public NamingEnumeration listBindings(String name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#destroySubcontext(javax.naming.Name)
		 */
		public void destroySubcontext(Name name) throws NamingException
		{
		}

		/**
		 * @see javax.naming.Context#destroySubcontext(java.lang.String)
		 */
		public void destroySubcontext(String name) throws NamingException
		{
		}

		/**
		 * @see javax.naming.Context#createSubcontext(javax.naming.Name)
		 */
		public Context createSubcontext(Name name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#createSubcontext(java.lang.String)
		 */
		public Context createSubcontext(String name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#lookupLink(javax.naming.Name)
		 */
		public Object lookupLink(Name name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#lookupLink(java.lang.String)
		 */
		public Object lookupLink(String name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#getNameParser(javax.naming.Name)
		 */
		public NameParser getNameParser(Name name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#getNameParser(java.lang.String)
		 */
		public NameParser getNameParser(String name) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#composeName(javax.naming.Name, javax.naming.Name)
		 */
		public Name composeName(Name arg0, Name arg1) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#composeName(java.lang.String, java.lang.String)
		 */
		public String composeName(String arg0, String arg1) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#addToEnvironment(java.lang.String, java.lang.Object)
		 */
		public Object addToEnvironment(String arg0, Object arg1) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#removeFromEnvironment(java.lang.String)
		 */
		public Object removeFromEnvironment(String arg0) throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#getEnvironment()
		 */
		public Hashtable getEnvironment() throws NamingException
		{
			return null;
		}

		/**
		 * @see javax.naming.Context#close()
		 */
		public void close() throws NamingException
		{
			this.referenceMap.clear();
		}

		/**
		 * @see javax.naming.Context#getNameInNamespace()
		 */
		public String getNameInNamespace() throws NamingException
		{
			return null;
		}
	}
}
