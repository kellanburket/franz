//
//  Consumer.swift
//  Pods
//
//  Created by Kellan Cummings on 2/5/16.
//y
//

import Foundation

/**
    Base consumer delegate. Used by SimpleConsumer.
*/
@objc public protocol ConsumerDelegate {
    /**
        Called when the consumer has consumed a new Message
     
        - Parameter message:  the returned message
        - Parameter offset:   the message offset
     */
    func consumerDidReturnMessage(_ message: Message, offset: Int64)

    /**
        Called when a fetch request has failed and cannot be retried.

        - Parameter topic:      the topic requested from the server
        - Parameter partition:  the partition requested from the server
        - Parameter offset:     the last message offset requested from server
        - Parameter errorId:    the error Id returned from the server
        - Parameter errorDescription:   a description of the error returned from the server
     */
    @objc optional func fetchDidFail(
        _ topic: String,
        partition: Int32,
        errorId: Int16,
        errorDescription: String
    )

    /**
        Called when a fetch has failed. Gives client the chance to retry before shutting down.
     
        - Parameter topic:      the topic requested from the server
        - Parameter partition:  the partition requested from the server
        - Parameter offset:     the last message offset requested from server
        - Parameter errorId:    the error Id returned from the server
        - Parameter errorDescription:   a description of the error returned from the server
     
        - Returns:  true if broker should attempt to retry request, false if not
     */
    @objc optional func shouldRetryFailedFetch(
        _ topic: String,
        partition: Int32,
        errorId: Int16,
        errorDescription: String
    ) -> Bool
    
    /**
        Called when the Consumer is ready to starting issuing pull requests.
     
        - Parameter consumer:   a Consumer
    */
    func consumerIsReady(_ consumer: Consumer)

    /**
        Called if a Leader is not found for a topic-partition
     
        - Parameter topic:      the topic
        - Parameter partition:  the partition
    */
    @objc optional func topicPartitionLeaderNotFound(_ topic: String, partition: Int32)
}


/**
    High-level consumer delegate. Used by HighLevelConsumer.
 */
@objc public protocol HighLevelConsumerDelegate: ConsumerDelegate {
    /**
        Called after messages have been pulled for server.
     
        - Parameter topic: the topic
        - Parameter partition: the partition
        - Parameter offset: the offset
     
        - Returns:  true if offset should be committed, false if otherwise
    */
    @objc optional func shouldCommitOffset(_ topic: String, partition: Int32, offset: Int64) -> Bool

    /**
        Called after messages have been pulled for server.

        - Parameter topic: the topic
        - Parameter partition: the partition
        - Parameter offset: the offset

        - Returns:  additional metadata to send with offset commit to server
    */
    @objc optional func shouldAttachOffsetMetadata(_ topic: String, partition: Int32, offset: Int64) -> String?

    /**
         Called after offset has been successfully committed
         
        - Parameter topic: the topic
        - Parameter partition: the partition
        - Parameter offset: the offset
    */
    @objc optional func offsetDidCommit(_ topic: String, partition: Int32, offset: Int64)

    /**
        Called if offset commit has failed and cannot be retried.

        - Parameter topic: the topic
        - Parameter partition: the partition
        - Parameter offset: the offset
        - Parameter errorId: error code id
        - Parameter errorDescription: description of the error
    */
    @objc optional func offsetCommitDidFail(_ topic: String, partition: Int32, offset: Int64, errorId: Int16, errorDescription: String)

    /**
        Called if offset commit has failed and commit is retriable

        - Parameter topic: the topic
        - Parameter partition: the partition
        - Parameter offset: the offset
        - Parameter errorId: error code id
        - Parameter errorDescription: description of the error

        - Returns: true if offset commit should be retried, false if otherwise
    */
    @objc optional func shouldRetryFailedOffsetCommit(_ topic: String, partition: Int32, offset: Int64, errorId: Int16, errorDescription: String) -> Bool
}


/*
    Base consumer class
*/
open class Consumer: NSObject {
    internal var broker: Broker?

    fileprivate var _topic: String
    fileprivate var _partition: Int32
    fileprivate var _clientId: String
    
    internal init(topic: String, partition: Int32, clientId: String) {
        self._topic = topic
        self._partition = partition
        self._clientId = clientId
    }
}


/*
    Class implementing a simple consumer model.
*/
open class SimpleConsumer: Consumer {
    /**
        the delegate
     */
    open var delegate: ConsumerDelegate
    
    internal init(
        topic: String,
        partition: Int32,
        clientId: String,
        delegate: ConsumerDelegate
    ) {
        self.delegate = delegate
        super.init(topic: topic, partition: partition, clientId: clientId)
    }

    /**
        Poll for messages
        
        Parameter offset:   starting offset
     */
    open func poll(_ offset: Int64) {
        if let coordinator = broker {
            coordinator.poll(
                _topic,
                partition: _partition,
                offset: offset,
                clientId: _clientId,
                replicaId: ReplicaId.none,
                { offset, messages in
                    for (idx, message) in messages.enumerated() {
                        self.delegate.consumerDidReturnMessage(
                            message,
                            offset: Int64(idx) + offset
                        )
                    }
                },
                { error in
                    if error.retriable {
                        if self.delegate.shouldRetryFailedFetch?(
                            self._topic,
                            partition: self._partition,
                            errorId: error.code,
                            errorDescription: error.description
                        ) != nil {
                            self.poll(offset)
                        }
                    } else {
                        self.delegate.fetchDidFail?(
                            self._topic,
                            partition: self._partition,
                            errorId: error.code,
                            errorDescription: error.description
                        )
                    }
                }
            )
        }
    }
}


/**
    Class implementing a high-level consumer. Managed by a group coordinator.
    The delegate is called after each fetch to
*/
open class HighLevelConsumer: Consumer {

    /**
        The Delegate
     */
    open var delegate: HighLevelConsumerDelegate

    internal var membership: GroupMembership?
    
    internal init(
        topic: String,
        partition: Int32,
        clientId: String,
        delegate: HighLevelConsumerDelegate
    ) {
        self.delegate = delegate
        super.init(topic: topic, partition: partition, clientId: clientId)
    }

    /**
        Poll for messages
     */
    open func poll() {
        if let groupId = membership?.group.id {
            if let coordinator = broker {
                do {
                    try coordinator.poll(
                        _topic,
                        partition: _partition,
                        groupId: groupId,
                        clientId: _clientId,
                        replicaId: ReplicaId.none,
                        {  offset, messages in
                            for (idx, message) in messages.enumerated() {
                                self.delegate.consumerDidReturnMessage(
                                    message,
                                    offset: offset + Int64(idx)
                                )
                            }
                            
                            if self.delegate.shouldCommitOffset != nil && self.delegate.shouldCommitOffset!(
                                self._topic,
                                partition: self._partition,
                                offset: offset
                            ) {
                                let metadata = self.delegate.shouldAttachOffsetMetadata?(
                                    self._topic,
                                    partition: self._partition,
                                    offset: offset
                                )
                                
                                coordinator.commitGroupOffset(
                                    groupId,
                                    topic: self._topic,
                                    partition: self._partition,
                                    offset: offset,
                                    metadata: metadata,
                                    clientId: self._clientId,
                                    {
                                        self.delegate.offsetDidCommit?(
                                            self._topic,
                                            partition: self._partition,
                                            offset: offset
                                        )
                                    },
                                    { error in
                                        if error.retriable {
                                            if self.delegate.shouldRetryFailedOffsetCommit?(
                                                self._topic,
                                                partition: self._partition,
                                                offset: offset,
                                                errorId: error.code,
                                                errorDescription: error.description
                                            ) != nil {
                                                self.poll()
                                            }
                                        } else {
                                            self.delegate.offsetCommitDidFail?(
                                                self._topic,
                                                partition: self._partition,
                                                offset: offset,
                                                errorId: error.code,
                                                errorDescription: error.description
                                            )
                                        }
                                    }
                                )
                            }
                        } , { error in
                            if error.retriable {
                                if self.delegate.shouldRetryFailedFetch?(
                                    self._topic,
                                    partition: self._partition,
                                    errorId: error.code,
                                    errorDescription: error.description
                                ) != nil {
                                   self.poll()
                                }
                            } else {
                                self.delegate.fetchDidFail?(
                                    self._topic,
                                    partition: self._partition,
                                    errorId: error.code,
                                    errorDescription: error.description
                                )
                            }
                        }
                    )
                } catch {
                    print("Unable to find consumer group with id '\(groupId)'")
                }
            } else {
                print("Cannot poll without broker")
            }
        } else {
            print("Cannot poll without group membership id")
        }
    }
}
