package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class CodeGuruJavaBestPracticeExamples {
    public boolean terminateInstanceNegative(final String instanceId, final AmazonEC2 ec2Client)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                DescribeInstanceStatusResult describeInstanceStatusResult = ec2Client.describeInstanceStatus(
                        new DescribeInstanceStatusRequest().withInstanceIds(instanceId).withIncludeAllInstances(true));
                List<InstanceStatus> instanceStatusList = describeInstanceStatusResult.getInstanceStatuses();
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if (timeElapsed > INSTANCE_TERMINATION_TIMEOUT) {
                    break;
                }
                if (instanceStatusList.size() < 1) {
                    Thread.sleep(WAIT_FOR_TRANSITION_INTERVAL);
                    continue;
                }
                String currentState = instanceStatusList.get(0).getInstanceState().getName();
                if ("shutting-down".equals(currentState) || "terminated".equals(currentState)) {
                    return true;
                } else {
                    Thread.sleep(WAIT_FOR_TRANSITION_INTERVAL);
                }
            } catch (AmazonServiceException ex) {
                throw ex;
            }
        }
        return false;
    }

    public void terminateInstancePositive(final String instanceId, final AmazonEC2 ec2Client)
            throws InterruptedException {
        Waiter<DescribeInstancesRequest> waiter = ec2Client.waiters().instanceTerminated();
        ec2Client.terminateInstances(new TerminateInstancesRequest().withInstanceIds(instanceId));
        try {
            waiter.run(new WaiterParameters()
                    .withRequest(new DescribeInstancesRequest().withInstanceIds(instanceId))
                    .withPollingStrategy(new PollingStrategy(new MaxAttemptsRetryStrategy(60),
                            new FixedDelayStrategy(5))));

        } catch (WaiterTimedOutException e) {
            List<InstanceStatus> instanceStatusList = ec2Client.describeInstanceStatus(
                    new DescribeInstanceStatusRequest()
                            .withInstanceIds(instanceId)
                            .withIncludeAllInstances(true))
                    .getInstanceStatuses();
            String state;
            if (instanceStatusList != null && instanceStatusList.size() > 0) {
                state = instanceStatusList.get(0).getInstanceState().getName();
                if (!Arrays.asList(InstanceStateName.ShuttingDown.toString(),
                        InstanceStateName.Terminated.toString()).contains(state)) {
                    LOGGER.error("WaiterTimedOutException: " + e);
                }
            }
        }
    }

    private void uploadInputStreamToS3Negative(String bucketName, InputStream input,
                                               String folderName, String s3FileKey,
                                               ObjectMetadata metadata) throws SdkClientException {
        final AmazonS3 amazonS3Client;
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(bucketName, folderName + "/" + s3FileKey, input,
                        metadata);
        amazonS3Client.putObject(putObjectRequest);
        log.info("Item with object Key {} has been uploaded", s3FileKey);
    }

    private void uploadInputStreamToS3Positive(String bucketName, InputStream input,
                                               String folderName, String s3FileKey,
                                               ObjectMetadata metadata) throws SdkClientException {
        final AmazonS3 amazonS3Client;
        final Integer READ_LIMIT = 10000;
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(bucketName, folderName + "/" + s3FileKey, input,
                        metadata);
        putObjectRequest.getRequestClientOptions().setReadLimit(READ_LIMIT);
        amazonS3Client.putObject(putObjectRequest);
        log.info("Item with object Key {} has been uploaded", s3FileKey);
    }

    public void flushNegative(final String sqsEndPoint,
                              final List<SendMessageBatchRequestEntry> batch)
    {
        final AmazonSQS amazonSqs;
        if (batch.isEmpty()) {
            return;
        }
        amazonSqs.sendMessageBatch(sqsEndPoint, batch);
        batch.clear();
    }

    public void flushPositive(final String sqsEndPoint,
                              final List<SendMessageBatchRequestEntry> batch)
    {
        final AmazonSQS amazonSqs;
        if (batch.isEmpty()) {
            return;
        }
        SendMessageBatchResult sendResult =
                amazonSqs.sendMessageBatch(sqsEndPoint, batch);

        final List<BatchResultErrorEntry> failed = sendResult.getFailed();
        if (!failed.isEmpty()) {
            final String failedMessage = failed.stream()
                    .map(batchResultErrorEntry -> String.format("messageId:%s failedReason:%s",
                            batchResultErrorEntry.getId(), batchResultErrorEntry.getMessage()))
                    .collect(Collectors.joining(","));
            throw new SQSUpdateException("Error occurred while sending messages to SQS::" + failedMessage);
        }
    }
}