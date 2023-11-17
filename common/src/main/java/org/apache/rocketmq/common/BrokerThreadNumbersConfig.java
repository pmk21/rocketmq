package org.apache.rocketmq.common;

import static org.apache.rocketmq.common.BrokerConfig.PROCESSOR_NUMBER;

public class BrokerThreadNumbersConfig {
    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPutMessageFutureThreadPoolNums() {
        return putMessageFutureThreadPoolNums;
    }

    public void setPutMessageFutureThreadPoolNums(int putMessageFutureThreadPoolNums) {
        this.putMessageFutureThreadPoolNums = putMessageFutureThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getLitePullMessageThreadPoolNums() {
        return litePullMessageThreadPoolNums;
    }

    public void setLitePullMessageThreadPoolNums(int litePullMessageThreadPoolNums) {
        this.litePullMessageThreadPoolNums = litePullMessageThreadPoolNums;
    }

    public int getAckMessageThreadPoolNums() {
        return ackMessageThreadPoolNums;
    }

    public void setAckMessageThreadPoolNums(int ackMessageThreadPoolNums) {
        this.ackMessageThreadPoolNums = ackMessageThreadPoolNums;
    }

    public int getProcessReplyMessageThreadPoolNums() {
        return processReplyMessageThreadPoolNums;
    }

    public void setProcessReplyMessageThreadPoolNums(int processReplyMessageThreadPoolNums) {
        this.processReplyMessageThreadPoolNums = processReplyMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public void setQueryMessageThreadPoolNums(int queryMessageThreadPoolNums) {
        this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
        this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
    }

    public int getLoadBalanceProcessorThreadPoolNums() {
        return loadBalanceProcessorThreadPoolNums;
    }

    public void setLoadBalanceProcessorThreadPoolNums(int loadBalanceProcessorThreadPoolNums) {
        this.loadBalanceProcessorThreadPoolNums = loadBalanceProcessorThreadPoolNums;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
        this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
    }

    public int getRecoverThreadPoolNums() {
        return recoverThreadPoolNums;
    }

    public void setRecoverThreadPoolNums(int recoverThreadPoolNums) {
        this.recoverThreadPoolNums = recoverThreadPoolNums;
    }

    public int getEndTransactionThreadPoolNums() {
        return endTransactionThreadPoolNums;
    }

    public void setEndTransactionThreadPoolNums(int endTransactionThreadPoolNums) {
        this.endTransactionThreadPoolNums = endTransactionThreadPoolNums;
    }

    private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
    private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int ackMessageThreadPoolNums = 3;
    private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

    private int adminBrokerThreadPoolNums = 16;
    private int clientManageThreadPoolNums = 32;
    private int consumerManageThreadPoolNums = 32;
    private int loadBalanceProcessorThreadPoolNums = 32;
    private int heartbeatThreadPoolNums = Math.min(32, PROCESSOR_NUMBER);
    private int recoverThreadPoolNums = 32;

    private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2,
            sendMessageThreadPoolNums * 4);

}
