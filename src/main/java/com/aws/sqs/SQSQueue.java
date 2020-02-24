package com.aws.sqs;

import com.amazonaws.auth.policy.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This demonstrates using sqs queue, LIFO Queue, Standard queue to send and receive messages.
 * Also adds ability to purge the queue and remove messages from the queue also
 * sends batch messages to the queue.
 */

public class SQSQueue {

    public static String testQueue = "sample-test-queue";
    private static String testQueueURL = "https://sqs.us-east-1.amazonaws.com/141793718810/sample-test-queue";
    public static String testFifoQueue = "sample-test-fifo-queue.fifo";
    private static String testFifoQueueURL = "https://sqs.us-east-1.amazonaws.com/141793718810/sample-test-fifo-queue.fifo";
    public static String deadLetterQueue = "sample-test-dead-letter-queue";
//    public static String deadLetterQueue = "CpHtmlToPngQueue-dead-letter-queue-dev";
    private static String deadLetterQueueURL = "https://sqs.us-east-1.amazonaws.com/141793718810/sample-test-dead-letter-queue";
    public static String mainQueue = "main-queue";
    private static String mainQueueURL = "https://sqs.us-east-1.amazonaws.com/141793718810/main-queue";
//    private static String mainQueueURL = "https://sqs.us-east-1.amazonaws.com/077994149108/CpHtmlToPngQueue-dev";
    private static String mainQueueARN = "arn:aws:sqs:us-east-1:141793718810:main-queue";
    private static String snsTopicARN = "arn:aws:sns:us-east-1:141793718810:Subscription";

    // Need to setup DLQ for staging and prod main queues.
    // Update the queue policy to support SQS once we need to integrate real time notification.

    public static void main(String[] args) {
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
//        String queues[] = {testQueue, testFifoQueue, mainQueue};

//        for (String queue : queues ) {
//            boolean isFifo = queue.endsWith(".fifo");
//            System.out.println(createSQSQueue(sqs, queue, isFifo));
//        }
//        // Create a dead letter queue for the main queue.
//        System.out.println(createSQSQueue(sqs, deadLetterQueue, false));
//        System.out.println(createMainQueueForDeadLetterQueue(sqs, mainQueue));
        // Get Queue attributes
//        List<String> attributes = Arrays.asList("All");

//        // Update queue attribute
        Map<String, String> updatedAttributes = new HashMap<String, String>();
//        updatedAttributes.put("MaximumMessageSize", "131072");
//        updatedAttributes.put("VisibilityTimeout", "15");

        final Gson gson = new GsonBuilder().create();

        final Map<String, String> readWritePolicy = new HashMap<String, String>();

        readWritePolicy.put("deadLetterTargetArn", System.getenv("DEAD_LETTER_QUEUE_ARN"));
        readWritePolicy.put("maxReceiveCount", "10");

        updatedAttributes.put("RedrivePolicy", gson.toJson(readWritePolicy));

        updateQueueAttributes(sqs, mainQueueURL, updatedAttributes);
//        // List all queues
//        List<String> testQueues = listQueues(sqs);
//
//        for (final String queueUrl : testQueues) {
//            System.out.println("  QueueUrl: " + queueUrl);
//            System.out.println(getQueueAttributes(sqs, queueUrl, attributes).toString());
//        }
//        System.out.println();

//        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
//
//        messageAttributes.put(
//            "Title",
//            new MessageAttributeValue().withDataType("String").withStringValue("My Message Title")
//        );
//        messageAttributes.put(
//            "Author",
//            new MessageAttributeValue().withDataType("String").withStringValue("Tonye Jack")
//        );
//
//        messageAttributes.put(
//            "Time",
//            new MessageAttributeValue().withDataType("Number").withStringValue("6")
//        );
//
//        sendMessageToQueue(sqs, "This is my first sqs message", messageAttributes, mainQueueURL);

        // Wait for 10 seconds.
//        sleep(10000L)
//
        // Send Batch messages to queue
//        SendMessageBatchRequestEntry messageBatchRequestEntry1 = new SendMessageBatchRequestEntry();
//        SendMessageBatchRequestEntry messageBatchRequestEntry2 = new SendMessageBatchRequestEntry();
//        SendMessageBatchRequestEntry messageBatchRequestEntry3 = new SendMessageBatchRequestEntry();
//
//        messageBatchRequestEntry1.setId("First");
//        messageBatchRequestEntry1.setMessageBody("This is my first batch message.");
//        messageBatchRequestEntry2.setId("Second");
//        messageBatchRequestEntry2.setMessageBody("This is my Second batch message.");
//        messageBatchRequestEntry3.setId("Third");
//        messageBatchRequestEntry3.setMessageBody("This is my Third batch message.");
//
//        List<SendMessageBatchRequestEntry> messages = Arrays.asList(
//            messageBatchRequestEntry1, messageBatchRequestEntry2, messageBatchRequestEntry3
//        );
//
//        sendBatchMessagesToQueue(sqs, messages, mainQueueURL);
//
//        sleep(10000L);
//
//        processMessageFromQueue(sqs, mainQueueURL);
//        // Purge the queue removing all messages in the queue.
//        purgeQueue(sqs, mainQueueURL);

        // Update the queue policy
//        updateQueuePolicy(sqs, mainQueueURL, snsTopicARN, mainQueueARN);
    }

    public static void purgeQueue(AmazonSQS sqs, String queueURL) {
        sqs.purgeQueue(new PurgeQueueRequest().withQueueUrl(queueURL));
    }

    public static void changeMessageVisibilityTimeOut(
        AmazonSQS sqs, String queueURL, String receiptHandle, Integer newVisibilityTimeout
    ) {
        sqs.changeMessageVisibility(
            new ChangeMessageVisibilityRequest()
                    .withQueueUrl(queueURL).withReceiptHandle(receiptHandle)
                    .withVisibilityTimeout(newVisibilityTimeout)
        );
    }

    public static void processMessageFromQueue(AmazonSQS sqs, String queueURL) {
        List<Message> queuedMessages = pollQueueForMessages(sqs, queueURL);

        if (!queuedMessages.isEmpty()) {
            for (Message message : queuedMessages) {
                System.out.println("Processing Message");
                System.out.println("  MessageId:     " + message.getMessageId());
                System.out.println("  ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("  MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("  Body:          " + message.getBody());
                for (final Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                    System.out.println("Attribute");
                    System.out.println("  Name:  " + entry.getKey());
                    System.out.println("  Value: " + entry.getValue());
                }
                // Delete message from queue
//                deleteMessageFromQueue(sqs, queueURL, message.getReceiptHandle());
                // Change message visibility Timeout
//                changeMessageVisibilityTimeOut(sqs, queueURL, message.getReceiptHandle(), 10);
            }
        }
    }

    public static void deleteMessageFromQueue(AmazonSQS sqs, String queueURL, String receiptHandle) {
        System.out.println("Deleting message with receiptHandler: " + receiptHandle);
        sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueURL).withReceiptHandle(receiptHandle));
    }

    public static List<Message> pollQueueForMessages(AmazonSQS sqs, String queueURL) {
        System.out.println("Receiving message from: " + queueURL + ".\n");
        return sqs.receiveMessage(
            new ReceiveMessageRequest().withQueueUrl(queueURL).withMaxNumberOfMessages(10)
        ).getMessages();
    }

    public static String sendBatchMessagesToQueue(
        AmazonSQS sqs,
        List<SendMessageBatchRequestEntry> messages,
        String queueURL
    ) {
        System.out.println("Sending a batch messages to: " + queueURL + ".\n");
        return sqs.sendMessageBatch(queueURL, messages).toString();
    }

    public static void sleep(long milliseconds) throws InterruptedException {
        // Wait for x number seconds.
        System.out.println("Waiting for " + milliseconds/1000 + " seconds.");
        Thread.sleep(milliseconds);
    }

    public static String sendMessageToQueue(
        AmazonSQS sqs,
        String body,
        Map<String, MessageAttributeValue> messageAttributes,
        String queueURL
    ) {
        SendMessageRequest request = new SendMessageRequest();
        request.setDelaySeconds(5);
        request.withMessageBody(body);
        request.withQueueUrl(queueURL);
        request.withMessageAttributes(messageAttributes);
        System.out.println(
            "Sending a message:" + request.getMessageBody().substring(0, 20) + ".\n"+
            "With a 5-second timer to: " + queueURL
        );

        return sqs.sendMessage(request).toString();
    }

    public static String updateQueueAttributes(AmazonSQS sqs, String queueURL, Map attributes) {
        System.out.println("Updating queue attributes: " + attributes.toString());
        return sqs.setQueueAttributes(queueURL, attributes).toString();
    }

    private static void updateQueuePolicy(AmazonSQS sqs, String queueURL, String topicARN, String queueARN) {
       Map<String, String> attributes = new HashMap<String, String>(1);
       Resource queueResource = new Resource(queueARN);
       Action actions = new Action() {
           @Override
           public String getActionName() {
               return "SQS:SendMessage";
           }
       };
       Statement mainQueueStatements = new Statement(Statement.Effect.Allow)
               .withActions(actions)
               .withPrincipals(Principal.All)
               .withResources(queueResource)
               .withConditions(
                   new Condition()
                           .withType("ArnEquals")
                           .withConditionKey("aws:SourceArn")
                           .withValues(topicARN)
               );
       final Policy mainQueuePolicy = new Policy().withId("MainQueuePolicy").withStatements(mainQueueStatements);

       attributes.put("Policy", mainQueuePolicy.toJson());

       updateQueueAttributes(sqs, queueURL, attributes);
    }

    public static Map<String, String> getQueueAttributes(AmazonSQS sqs, String queueURL, List<String> attributes) {
        return sqs.getQueueAttributes(queueURL, attributes).getAttributes();
    }

    public static List listQueues(AmazonSQS sqs) {
        System.out.println("Listing all queues in your account.\n");
        return sqs.listQueues().getQueueUrls();
    }

    public static String createMainQueueForDeadLetterQueue(AmazonSQS sqs, String queueName) {
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        final Gson gson = new GsonBuilder().create();

        final Map<String, String> readWritePolicy = new HashMap<String, String>();

        readWritePolicy.put("deadLetterTargetArn", System.getenv("DEAD_LETTER_QUEUE_ARN"));
        readWritePolicy.put("maxReceiveCount", "3");

        final Map<String, String> attributes = new HashMap<String, String>();

        attributes.put("DelaySeconds", "0");
        attributes.put("MaximumMessageSize", "262144");
        attributes.put("VisibilityTimeout", "30");
        attributes.put("MessageRetentionPeriod", "345680");
        attributes.put("ReceiveMessageWaitTimeSeconds", "0");
        attributes.put("RedrivePolicy", gson.toJson(readWritePolicy));

        createQueueRequest.setAttributes(attributes);

        System.out.println("Creating a dead letter new standard SQS queue called " + queueName + ".\n");

        return createORRecreateQueue(sqs, createQueueRequest, true);
    }


    private static String findQueue(AmazonSQS sqs, String queueName) {
        try {
            System.out.println("Finding : " + queueName);
            return sqs.getQueueUrl(queueName).getQueueUrl();
        } catch (AmazonSQSException ae) {
            System.out.println("Queue doesn't exists: " + queueName);
            return null;
        }
    }

    private static void reCreateQueue(AmazonSQS sqs, String queueName) {
        try {
            System.out.println("Recreating : " + queueName);
            String queue_url = findQueue(sqs, queueName);
            System.out.println("Deleting queue: " + queue_url);
            sqs.deleteQueue(queue_url);
        } catch (AmazonSQSException ae) {
            System.out.println("Queue doesn't exists: " + queueName);
        }
    }

    private static String createORRecreateQueue(AmazonSQS sqs, CreateQueueRequest createQueueRequest, boolean reCreate) {
        if (reCreate) {
            String queueName = createQueueRequest.getQueueName();
            reCreateQueue(sqs, queueName);
        }

        try {
            return sqs.createQueue(createQueueRequest).getQueueUrl();
        } catch (AmazonSQSException e) {
            System.out.println("Error creating queue: " +  e.getErrorMessage());
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return "";
    }

    /**
     * Creates an sqs queue.
     * @param sqs:
     *          The sqs client.
     * @param queueName:
     *          The queueName
     * */

    private static String createSQSQueue(AmazonSQS sqs, String queueName, boolean isFifo) throws AmazonSQSException {

        final CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);

        if (isFifo) {
            // Create a FIFO queue
            final Map<String, String> attributes = new HashMap<String, String>();

            attributes.put("FifoQueue", "true");

            // If the user doesn't provide a MessageDeduplicationId,
            // generate a MessageDeduplicationId based on the content.
            attributes.put("ContentBasedDeduplication", "true");

            createQueueRequest.setAttributes(attributes);

            System.out.println("Creating a new Amazon SQS FIFO queue called " + queueName +".\n");
        } else {
            System.out.println("Creating a new standard SQS queue called " + queueName + ".\n");
        }

        return createORRecreateQueue(sqs, createQueueRequest, false);
    }
}
