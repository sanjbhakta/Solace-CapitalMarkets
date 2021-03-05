"""Clearing & settlement of financial transactions from a Solace topic."""

import ast
import os
import platform
import time

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from jproperties import Properties

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

TOPIC_PREFIX = "SOLACE/CAPITALMARKETS/TRANSACTION/SETTLE"


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


# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: InboundMessage):
        payload_str = message.get_payload_as_string()
        #print("\n" + f"Message Payload String: {payload_str} \n")

        # No fraud detected, reconcile and settle transaction
        print("Transaction details: ", payload_str, "\n")
        print("Reconcilliation completed\n")

    
# Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
# Note: The reconnections strategy could also be configured using the broker properties object
messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

# Blocking connect thread
messaging_service.connect()
print(f'Messaging Service connected? {messaging_service.is_connected}')

# Error Handeling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Define a Topic subscriptions 
topics = [TOPIC_PREFIX + "/python/>", TOPIC_PREFIX + "/python/v2/>"]
#topics = [TOPIC_PREFIX ]

topics_sub = []
for t in topics:
    topics_sub.append(TopicSubscription.of(t))

# Build a Receiver with the given topics and start it
direct_receiver = messaging_service.create_direct_message_receiver_builder()\
                        .with_subscriptions(topics_sub)\
                        .build()

direct_receiver.start()
print(f'Direct Subscriber is running? {direct_receiver.is_running()}')

try:
    print(f"Subscribing to: {topics}")
    # Callback for received messages
    direct_receiver.receive_async(MessageHandlerImpl())
    try: 
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
finally:
    print('\nTerminating receiver')
    direct_receiver.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()

