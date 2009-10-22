package net.sf.hajdbc;

import java.util.Collections;
import java.util.ResourceBundle;

import org.junit.Assert;
import org.junit.Test;

public class MessagesTest
{
   @Test
   public void verifyEnum()
   {
      for (Messages value: Messages.values())
      {
         Assert.assertNotNull(value.name(), value.getMessage());
      }
   }
   
   @Test
   public void verifyResourceBundle()
   {
      ResourceBundle resource = ResourceBundle.getBundle(Messages.class.getName());
      
      for (String key: Collections.list(resource.getKeys()))
      {
         boolean found = false;
         
         for (Messages value: Messages.values())
         {
            if (value.toString().equals(key))
            {
               found = true;
               break;
            }
         }
         
         Assert.assertTrue(key, found);
      }
   }
}
