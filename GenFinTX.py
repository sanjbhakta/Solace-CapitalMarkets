"""Produce financial transactions into a Solace topic."""

import os
import platform
import time

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from transactions import create_random_transaction
from jproperties import Properties


if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

MSG_COUNT = 5
#TOPIC_PREFIX = "samples/hello"
TOPIC_PREFIX = "SOLACE/CAPITALMARKETS/TRANSACTION"

# Inner classes for error handling
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")
    
    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

 
class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")

# Broker Config. Note: Could pass other properties Look into
solace_configs = Properties()
with open('solace.properties', 'rb') as read_prop: 
    solace_configs.load(read_prop)

broker_props = {
    "solace.messaging.transport.host": solace_configs.get("SOLACE_HOST").data,
    "solace.messaging.service.vpn-name": solace_configs.get('SOLACE_VPN').data,
    "solace.messaging.authentication.scheme.basic.username": solace_configs.get("SOLACE_USERNAME").data,
    "solace.messaging.authentication.scheme.basic.password": solace_configs.get("SOLACE_PASSWORD").data
    }

# Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
# Note: The reconnections strategy could also be configured using the broker properties object
messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

# Blocking connect thread
messaging_service.connect()
print(f'Messaging Service connected? {messaging_service.is_connected}')

# Event Handling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Create a direct message publisher and start it
direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
direct_publisher.set_publish_failure_listener(PublisherErrorHandling())

# Blocking Start thread
direct_publisher.start()
print(f'Direct Publisher ready? {direct_publisher.is_ready()}')

# Prepare outbound message payload and body
# message_body = "this is the body of the msg"

outbound_msg_builder = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \

count = 1
print("\nSend a KeyboardInterrupt to stop publishing\n")
try: 
    while True:
        while count <= MSG_COUNT:
            topic = Topic.of(TOPIC_PREFIX + f'/python/{count}')
            #topic = Topic.of(TOPIC_PREFIX )

            transaction: dict = create_random_transaction()
            message_body = str(transaction)

            # Direct publish the message with dynamic headers and payload
            outbound_msg = outbound_msg_builder \
                            .with_application_message_id(f'NEW {count}')\
                            .build(f'{message_body}')
            direct_publisher.publish(destination=topic, message=outbound_msg)

            print(f'Published message on {topic}')
            count += 1
            time.sleep(0.1)
        print("\n")
        count = 1
        time.sleep(1)

except KeyboardInterrupt:
    print('\nTerminating Publisher')
    direct_publisher.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()



