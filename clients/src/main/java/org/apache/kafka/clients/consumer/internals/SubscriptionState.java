/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    /**
     * 内部枚举类，表示订阅 Topic 的模式
     */
    private enum SubscriptionType {
        NONE, // SubscriptionState.subscriptionType 的初始值
        AUTO_TOPICS,  // 按照指定的 Topic 名字进行订阅，自动分配分区
        AUTO_PATTERN, // 按照指定的正则表达式匹配 Topic 进行订阅，自动分配分区
        USER_ASSIGNED // 用户手动指定消费者消费的 Topic 以及分区编号
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    /*
     * 使用 AUTO_PATTERN 模式时，是按照此字段记录的正则表达式对所有 Topic 进行匹配
     * 对匹配符合的 Topic 进行订阅
     */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    /*
     * 如果使用 AUTO_PATTERN 或 AUTO_TOPICS 模式，则使用此集合记录所有订阅的 Topic。
     * 向 subscription 集合中添加数据的方法只有 changeSubscription() 方法，而调用
     * changeSubscription() 方法的地方有两处，可自行通过 idea 查看
     */
    private Set<String> subscription;

    /* the list of partitions the user has requested */
    /*
     * 如果使用 USER_ASSIGNED 模式，则此集合记录了分配给当前消费者的 TopicPartition 集合。
     * SubscriptionType 模式是互斥的，所以 userAssignment 集合与 subscription 集合也是互斥的
     */
    private Set<TopicPartition> userAssignment;

    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    /*
     * 之前说过 Consumer Group 会选举出一个 Leader，Leader 使用该集合记录 Consumer Group 中所有消费者订阅的
     * Topic，而其他 Follower 的该集合中只保存了其自身的订阅的 Topic
     */
    private final Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    /*
     * 无论使用什么订阅模式，都会使用该集合记录每个 TopicPartition 的消费状态
     */
    private final PartitionStates<TopicPartitionState> assignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    /*
     * 标记是否需要从 GroupCoordinator 获取最近提交的 offset。
     * 当出现异步提交 offset 操作或是 Rebalance 操作刚完成时会将其置为 true，
     * 成功获取最近提交 offset 之后会设置为 false
     */
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* Listener to be invoked when assignment changes */
    // 用于监听分区分配操作
    private ConsumerRebalanceListener listener;

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = Collections.emptySet();
        this.userAssignment = Collections.emptySet();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        // 如果是 None，则可以指定其他模式
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type) // 如果已经指定了其他模式，则会报错
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public void subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        /*
         * 用户未指定 ConsumerRebalanceListener 时，默认使用 NoOpConsumerRebalanceListener
         * 其中所有方法的实现都是空的
         */
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_TOPICS); // 选择 AUTO_TOPICS 模式

        this.listener = listener;

        changeSubscription(topics);
    }

    public void subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        changeSubscription(topics);
    }

    private void changeSubscription(Set<String> topicsToSubscribe) {
        // 订阅的 Topic 有变化
        if (!this.subscription.equals(topicsToSubscribe)) {
            this.subscription = topicsToSubscribe;
            this.groupSubscription.addAll(topicsToSubscribe);
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    public void groupSubscribe(Collection<String> topics) {
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        this.groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    public void resetGroupSubscription() {
        this.groupSubscription.retainAll(subscription);
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public void assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (!this.assignment.partitionSet().equals(partitions)) {
            this.userAssignment = partitions;

            Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
            for (TopicPartition partition : partitions) {
                TopicPartitionState state = assignment.stateValue(partition);
                if (state == null)
                    state = new TopicPartitionState();
                partitionToState.put(partition, state);
            }
            this.assignment.set(partitionToState);
            this.needsFetchCommittedOffsets = true;
        }
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");

        // after rebalancing, we always reinitialize the assignment value
        this.assignment.set(partitionToStateMap(assignments));
        this.needsFetchCommittedOffsets = true;
    }

    private Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
        return map;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.userAssignment = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    public Pattern getSubscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().paused) {
                paused.add(state.topicPartition());
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.partitionSet();
    }

    public List<TopicPartition> fetchablePartitions() {
        List<TopicPartition> fetchable = new ArrayList<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().isFetchable())
                fetchable.add(state.topicPartition());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().hasValidPosition())
                allConsumed.put(state.topicPartition(), new OffsetAndMetadata(state.value().position));
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions)
            if (!hasValidPosition(partition))
                return false;
        return true;
    }

    public boolean hasAllFetchPositions() {
        return hasAllFetchPositions(this.assignedPartitions());
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (!state.value().hasValidPosition())
                missing.add(state.topicPartition());
        }
        return missing;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public boolean hasValidPosition(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).hasValidPosition();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    public void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    // 表示的是 TopicPartition 的消费状态
    private static class TopicPartitionState {
        private Long position; // last consumed position
        private OffsetAndMetadata committed;  // last committed position
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
            this.resetStrategy = null;
        }

        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;
            this.position = null;
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        private void seek(long offset) {
            this.position = offset;
            this.resetStrategy = null;
        }

        private void position(long offset) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = offset;
        }

        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

}
