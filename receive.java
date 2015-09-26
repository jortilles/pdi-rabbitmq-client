/*
*  RabbitMQ consumer
*/

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
 
private FieldHelper outputField = null;
Connection connection = null;
Channel channel = null;
String encoding = "UTF-8";
String exchangeName = null;
String routingKey = null;
String queueName = null;
boolean autoAck = false; //false if the server should expect explicit acknowledgements
boolean durable = true;  //=survive server restart
int limite = 0;
int linea = 0;
 
 
public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
  if (first) {
    outputField = get(Fields.Out, getParameter("OUTPUT_FIELD"));
    first = false;
  }
 

  Object[] outputRow = createOutputRow(new Object[0], data.outputRowMeta.size());
 
  try {
 
    GetResponse response = channel.basicGet(queueName, autoAck);
 
	// i want to receive only X number of messages. This controls if i got the 10 messages i want to receive
	if( linea < limite  ){
		linea = linea + 1;
	}else{
		// stop transformation
		setOutputDone();
		return false;
	}
 
    if (response == null) {
      // No message retrieved => stop transformation
      setOutputDone();
      return false;
 
    } else {
      AMQP.BasicProperties props = response.getProps();
      String messageBody = new String (response.getBody(), encoding);
      long deliveryTag = response.getEnvelope().getDeliveryTag();
 
      outputField.setValue(outputRow, messageBody);
      putRow(data.outputRowMeta, outputRow);
      channel.basicAck(deliveryTag, false); // acknowledge receipt of the message
      return true;
    }
 
  } catch (IOException ex) {
    putError(data.outputRowMeta, outputRow, 1, ex.toString(), "","" );
    return true;
  }
}
 
 
public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
{
  if (parent.initImpl(stepMetaInterface, stepDataInterface)){
 
    try {
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
      channel.exchangeDeclare(exchangeName, "topic", durable);
 
      routingKey = getVariable("amqp.routing_key");
      queueName = getVariable("amqp.queue");
      boolean exclusive = false; // Exclusive queues may only be accessed by the current connection, and are deleted when that connection closes
      boolean autoDelete = false; //If true, the exchange is deleted when all queues have finished using it.
      channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);
      //String queueName = channel.queueDeclare().getQueue(); //a non-durable, exclusive, autodelete queue with a generated name
 
      channel.queueBind(queueName, exchangeName, routingKey);
 
 
      limite  =   Integer.parseInt( getVariable("limit.messages.to.get")  );

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
      logError("Exception: ", ex);
    }
  }
  parent.disposeImpl(smi, sdi);
}