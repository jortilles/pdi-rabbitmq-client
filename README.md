# pdi-rabbitmq-client
kettle java classes to interact with RabbitMQ

There are a couple of java classes to interact with RabbitMQ but they didn't work to me...

Now i publish my approach. 

There are two sets...

publish.java : The java class to publish messages

publish.ktr: a sample.

Publish require:

Configuration variables to be set:

amqp.user for instance my_user

amqp.host for instance 127.0.0.1

amqp.password for instance my_password

amqp.routing_key for instance my_routing_key

amqp.exchange for instance my_exchange

amqp.queue for instance my_queue




The message to publish: Input field message 	


receive.java: The hava class receive X number of messages

receive.ktr: a sample

Publish require:

Configuration variables to be set:

amqp.user for instance my_user

amqp.host for instance 127.0.0.1

amqp.password for instance my_password

amqp.routing_key for instance my_routing_key

amqp.exchange for instance my_exchange

amqp.queue for instance my_queue

limit.messages.to.get for instance 100

