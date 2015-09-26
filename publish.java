import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
 
private FieldHelper inputField = null;

private FieldHelper outputField = null;
//Integer firstnameIndex = null;
//Integer nameIndex = null;
//ConnectionFactory factory = null;
String encoding = "UTF-8";
String messageBody = null;
Connection connection = null;
String exchangeName = null;
String routingKey = null;
String queueName = null;
boolean durable = true; //=survive server restart
Channel channel = null;
 
 
 
public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
  // First, get a row from the default input hop
  Object[] r = getRow();
 
  // If the row object is null, we are done processing.
  if (r == null) {
    setOutputDone();
    return false;
  }
 
  // Let's look up parameters only once for performance reason.
  if (first) {
    inputField = get(Fields.In, getParameter("INPUT_FIELD"));

    first=false;
  }
 
  // It is always safest to call createOutputRow() to ensure that your output row's Object[] is large
  // enough to handle any new fields you are creating in this step.
  Object[] outputRow = createOutputRow(r, data.outputRowMeta.size());
 
  try{
    //RabbitMQ - send message
    // @throws java.io.IOException if an error is encountered
    messageBody = inputField.getString(r);

	logBasic( "Publicando...  Mensaje: " +  messageBody );


    channel.basicPublish(exchangeName, routingKey, null, messageBody.getBytes(encoding));


    return true;
 
  } catch (IOException ex) {

    putError(data.outputRowMeta, outputRow, 1, ex.toString(), "","" );
    return true;
  }
}
 

public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
{
  if (super.init(stepMetaInterface, stepDataInterface)) {
 
    try {

	 /*
	 logBasic( "AMQP Variables Username: " +  getVariable("amqp.user") + " Host: "+ getVariable("amqp.host")  
	 + " Pwd: "+ getVariable("amqp.password")    +  " Routing: " +  getVariable("amqp.routing_key") );
	*/


      ConnectionFactory factory = new ConnectionFactory();
      factory.setUsername(getVariable("amqp.user"));
      factory.setPassword(getVariable("amqp.password"));
      factory.setVirtualHost("/");
      factory.setHost(getVariable("amqp.host"));
      factory.setPort(5672);
 
      connection = factory.newConnection();
      channel = connection.createChannel();
 
      durable = true; //=survive server restart
      exchangeName = getVariable("amqp.exchange");
      routingKey = getVariable("amqp.routing_key");
      queueName = getVariable("amqp.queue");
      channel.exchangeDeclare(exchangeName, "topic", durable);
	  boolean exclusive = false; // Exclusive queues may only be accessed by the current connection, and are deleted when that connection closes
      boolean autoDelete = false; //If true, the exchange is deleted when all queues have finished using it.
      channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);
      channel.queueBind(queueName, exchangeName, routingKey);
 
      


      return true;
 
    } catch (InterruptedException ex) {
      logError("InterruptedException: ", ex);
      return false;
    } catch (NumberFormatException ex) {
      logError("NumberFormatException: ", ex);
      return false;
    } catch (ShutdownSignalException ex) {
      logError("ShutdownSignalException: ", ex);
      return false;
    } catch (ConsumerCancelledException ex) {
      logError("ConsumerCancelledException: ", ex);
      return false;
    } catch (NullPointerException ex) {
      logError("NullPointerException: ", ex);
      return false;
    } catch (Exception ex) {
      logError("Exception: ", ex);
      return false;
    }
  }
  return false;
}
 
public void dispose(StepMetaInterface smi, StepDataInterface sdi)
{
  if ((channel != null) || (connection != null)) {
    try{
      channel.close();
      connection.close();
    } catch (Exception ex) {
      logError("IOException: ", ex);
    }
  }
  parent.disposeImpl(smi, sdi);
}

