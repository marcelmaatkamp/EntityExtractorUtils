package spring

import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

class GroovyMessage implements Message { 
  String message 

  public static void main(final String[] args) throws Exception {
    def ctx = new ClassPathXmlApplicationContext("spring-beans.xml")
    def  messenger = (Message) ctx.getBean("messenger")
    println("Message from spring: " + messenger.message)
  }

}
