package com.sample;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.aws.sns.SNSService;

public class SetupSQSSNS {

    private final static String sqsQueue = "";
    private final static String snsTopic = "";
    private final static String lambdaFunctionARN = "";
    private final static String topicName = "Subscription";
    private static String snsTopicARN = "arn:aws:sns:us-east-1:141793718810:Subscription";
    private static String testEmailAddress = "";
    private static String testPhoneNumber = "";
    private static String mainQueueARN = "arn:aws:sqs:us-east-1:141793718810:main-queue";

    public static void main(String[] args) {
        // Add the sns client.
        AmazonSNS sns = AmazonSNSClientBuilder.defaultClient();
        final SNSService testSns = new SNSService().withSNSClient(sns).withTopicName(topicName);
        System.out.println(testSns.listTopics());
    }

}
