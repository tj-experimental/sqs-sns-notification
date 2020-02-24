package com.aws.sns;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.*;

import java.util.List;

public class SNSService {
    private AmazonSNS sns;
    private String topicName;
    private static String snsTopicARN = "arn:aws:sns:us-east-1:141793718810:Subscription";
    private static String testEmailAddress = "";
    private static String testPhoneNumber = "";
    private static String mainQueueARN = "arn:aws:sqs:us-east-1:141793718810:main-queue";

    public SNSService() {
    }

    public SNSService withSNSClient(AmazonSNS sns) {
        this.sns = sns;
        return this;
    }

    public SNSService withTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public static void main(String[] args) {
//        System.out.println("Running sns...");
//        System.out.println(createSNSTopic(sns, topicName));
//        System.out.println(listTopics(sns));
//        System.out.println(createEmailSubscription(sns, snsTopicARN, testEmailAddress));
//        System.out.println(createPhoneNumberSubscription(sns, snsTopicARN, testPhoneNumber));
//        System.out.println(getTopicAttributes(sns, snsTopicARN));
//        updateTopicAttributes(sns, snsTopicARN, "DisplayName", topicName + "-Changed");
//        deleteTopic(sns, snsTopicARN);
//        createSQSQueueSubscription(sns, snsTopicARN, mainQueueARN);
//        System.out.println(getTopicSubscriptions(sns, snsTopicARN));
//        System.out.println(checkIfPhoneNUmberOptedOut(sns, testPhoneNumber));
//        System.out.println(listOptedOutPhoneNumbers(sns));
//        optOutOfEmailSubscription(sns, snsTopicARN, testEmailAddress);
//        optOutOfSMSSubscription(sns, snsTopicARN, testPhoneNumber);
//        optInPhoneNumber(sns, testPhoneNumber);
//        System.out.println(publishMessageToSubscribers(sns, snsTopicARN, "Test Message from aws sns."));
    }

    public String createSNSTopic(AmazonSNS sns, String topicName) {
        System.out.println("Creating a new SNS topic for: " + topicName);
        return sns.createTopic(new CreateTopicRequest().withName(topicName)).getTopicArn();
    }

    public String listTopics(){
        System.out.println("Retrieving SNS Topics.");
        return this.sns.listTopics().toString();
    }

    public String getTopicAttributes(String topicARN){
        System.out.println("Retrieving Topic attribute...");
        return this.sns.getTopicAttributes(topicARN).toString();
    }

    public void unsubscribe(String subscriptionARN) {
        System.out.println("Unsubscribed from the SNS topic: " + subscriptionARN);
        this.sns.unsubscribe(new UnsubscribeRequest().withSubscriptionArn(subscriptionARN));
    }

    public static void deleteTopic(AmazonSNS sns, String topicARN) {
        System.out.println("Deleting sns topic: " + topicARN + " ...");
        sns.deleteTopic(topicARN).toString();
    }

    public static void updateTopicAttributes(AmazonSNS sns, String topicARN, String attributeName, String attributeValue) {
        System.out.println("Updating topic attribute \"" + attributeName  + "\": " + topicARN + ":- " + attributeValue + " ...");
        sns.setTopicAttributes(
            new SetTopicAttributesRequest()
                    .withTopicArn(topicARN)
                    .withAttributeName(attributeName)
                    .withAttributeValue(attributeValue)
        );
    }

    public static String createEmailSubscription(AmazonSNS sns, String topicARN, String emailAddress) {
        System.out.println("Creating an email subscription \"" + topicARN + "\" for: " + emailAddress + "...");
        return sns.subscribe(
                new SubscribeRequest()
                        .withTopicArn(topicARN)
                        .withProtocol("email")
                        .withEndpoint(emailAddress)
        ).toString();
    }

    public static String createPhoneNumberSubscription(AmazonSNS sns, String topicARN, String phoneNumber) {
        System.out.println("Creating an sms subscription \"" + topicARN + "\" for: " + phoneNumber + "...");
        return sns.subscribe(
                new SubscribeRequest()
                        .withTopicArn(topicARN)
                        .withProtocol("sms")
                        .withEndpoint(phoneNumber)
        ).toString();
    }

    public static String createSQSQueueSubscription(AmazonSNS sns, String topicARN, String sqsQueueARN) {
        System.out.println("Subscribing to sqs queue: " + sqsQueueARN);
        return sns.subscribe(
                new SubscribeRequest().withProtocol("sqs")
                        .withTopicArn(topicARN)
                        .withEndpoint(sqsQueueARN)
        ).toString();
    }

    public static List<Subscription> getTopicSubscriptions(AmazonSNS sns, String topicARN) {
        System.out.println("Retrieving subscriptions for topic: " + topicARN);
        return sns.listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest().withTopicArn(topicARN)).getSubscriptions();
    }

    public static String checkIfPhoneNUmberOptedOut(AmazonSNS sns, String phoneNumber) {
        System.out.println("Verifying if phone number opted out");
        return sns.checkIfPhoneNumberIsOptedOut(
                new CheckIfPhoneNumberIsOptedOutRequest().withPhoneNumber(phoneNumber)
        ).toString();
    }

    public static String listOptedOutPhoneNumbers(AmazonSNS sns) {
        System.out.println("Listing opted out phone numbers.");
        return sns.listPhoneNumbersOptedOut(new ListPhoneNumbersOptedOutRequest()).toString();
    }

    public void optOutOfEmailSubscription(String topicARN, String emailAddress) {
        final List<Subscription> subscriptions = getTopicSubscriptions(sns, topicARN);

        System.out.println("Opting out of subscription: " + topicARN);

        for (Subscription subscription : subscriptions) {
            if (
                subscription.getProtocol().equalsIgnoreCase("email") &&
                subscription.getEndpoint().equalsIgnoreCase(emailAddress)
            ) {
                System.out.println("Unsubscribed : \"" + emailAddress + "\" from " +  subscription.getTopicArn());
                this.unsubscribe(subscription.getSubscriptionArn());
            }
        }
    }

    public void optOutOfSMSSubscription(String topicARN, String phoneNumber) {
        final List<Subscription> subscriptions = getTopicSubscriptions(sns, topicARN);

        System.out.println("Opting out of subscription: " + topicARN);

        for (Subscription subscription : subscriptions) {
            if (
                    subscription.getProtocol().equalsIgnoreCase("sms") &&
                    subscription.getEndpoint().equalsIgnoreCase(phoneNumber)
            ) {
                System.out.println("Unsubscribed : \"" + phoneNumber + "\" from " +  subscription.getTopicArn());
                this.unsubscribe(subscription.getSubscriptionArn());
            }
        }
    }

    public static void optInPhoneNumber(AmazonSNS sns, String phoneNumber) {
        System.out.println("Opted In Phone number: " + phoneNumber + "to sns topic");
        sns.optInPhoneNumber(new OptInPhoneNumberRequest().withPhoneNumber(phoneNumber));
    }

    public static String  publishMessageToSubscribers(AmazonSNS sns, String topicARN , String message) {
        System.out.println("Sending a message to all subscribers.");
        return sns.publish(new PublishRequest().withTopicArn(topicARN).withMessage(message)).toString();
    }
}
