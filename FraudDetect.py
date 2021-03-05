"""Transaction surveillance, analyze for fraud after receiving financial transactions from a Solace topic."""


## Goal: Publisher + Subscriber 
import ast
import os
import time

# Import Solace Python  API modules
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
# from solace.messaging.core.solace_message import SolaceMessage
from solace.messaging.resources.topic import Topic
from jproperties import Properties

# Pub topic
TOPIC_TST = "SOLACE/CAPITALMARKETS/TRANSACTION/SETTLE"


#Sub topic
TOPIC_PREFIX = "SOLACE/CAPITALMARKETS/TRANSACTION/FRAUD_DETECT"

SHUTDOWN = False


# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: InboundMessage):
        global transaction_FX
        global tx_my

        payload_str = message.get_payload_as_string()
        transaction = ast.literal_eval(payload_str)

        #Receive the FX
        if transaction['amount'] >= 900:
            print("Fraud detected, sent to Compliance officer\n")
        else:
            print("No fraud\n")
            publish_mesg(str(transaction))


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

# Broker Config
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
# print(f'Messaging Service connected? {messaging_service.is_connected}')

# Error Handeling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

def publish_mesg(transaction_str):
    # Create a direct message publisher and start it
    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.set_publish_failure_listener(PublisherErrorHandling())
    direct_publisher.set_publisher_readiness_listener

    # Blocking Start thread
    direct_publisher.start()

    outbound_msg = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \
                .build(transaction_str)

    msgSeqNum = 1

    print("transaction details: ", str(transaction_str))

    direct_publisher.publish(destination=Topic.of(TOPIC_TST + f"/python/{msgSeqNum}"), message=outbound_msg)
    direct_publisher.terminate()



unique_name = ""
while not unique_name:
    unique_name = input("Enter your name: ").replace(" ", "")

# Define a Topic subscriptions 
topics = [TOPIC_PREFIX + "/python/>", TOPIC_PREFIX + "/control/>"]
topics_sub = []
for t in topics:
    topics_sub.append(TopicSubscription.of(t))



try:
    print(f"Subscribed to: {topics}")
    # Build a Receiver
    direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(topics_sub).build()
    direct_receiver.start()
    # Callback for received messages
    direct_receiver.receive_async(MessageHandlerImpl())
    if direct_receiver.is_running():
        print("Connected and Subscribed! Ready to publish\n")

    
    try:
        while not SHUTDOWN:            
              time.sleep(0.1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')

finally:
    print('Terminating Publisher and Receiver')
    direct_receiver.terminate()
    print('Disconnecting Messaging Service')
    messaging_service.disconnect()
