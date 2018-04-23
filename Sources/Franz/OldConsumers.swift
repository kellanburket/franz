//
//  OldConsumers.swift
//  Franz
//
//  Created by Luke Lau on 07/08/2017.
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
	func consumerIsReady(_ consumer: OldConsumer)
	
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
public class OldConsumer: NSObject {
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


/**
Class implementing a simple consumer model.
*/
@available(*, deprecated, message: "Use Consumer instead")
public class SimpleConsumer: OldConsumer {
	/**
	the delegate
	*/
	var delegate: ConsumerDelegate
	
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
	func poll(_ offset: Int64) {
		guard let coordinator = broker else {
			return
		}
		
		_ = coordinator.poll(topics: [_topic: [_partition: offset]], clientId: _clientId, replicaId: .none, callback: { topicName, partitionId, offset, messages in
			messages.forEach { self.delegate.consumerDidReturnMessage($0, offset: offset) }
		}, errorCallback: { error in
			switch error {
			case .fetchFailed, .noConnection, .noGroupMembershipForBroker:
				print("Failed polling with a non-kafka error")
			case .kafkaError(let errorCode):
				if errorCode.retriable, self.delegate.shouldRetryFailedFetch?(self._topic, partition: self._partition, errorId: errorCode.code, errorDescription: errorCode.description) ?? true {
					self.poll(offset)
				}
				self.delegate.fetchDidFail?(self._topic, partition: self._partition, errorId: errorCode.code, errorDescription: errorCode.description)
			}
		})
	}
}


/**
Class implementing a high-level consumer. Managed by a group coordinator.
The delegate is called after each fetch to
*/
@available(*, deprecated, message: "Use Consumer instead")
public class HighLevelConsumer: OldConsumer {
	
	/**
	The Delegate
	*/
	var delegate: HighLevelConsumerDelegate
	
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
	func poll() {
		guard let groupId = membership?.group.id else {
			print("Cannot poll without group membership id")
			return
		}
		guard let coordinator = broker else {
			print("Cannot poll without broker")
			return
		}
		
		_ = coordinator.poll(topics: [_topic: [_partition]], fromStart: true, groupId: groupId, clientId: _clientId, replicaId: .none, callback: { (topicName, partitionId, offset, messages) in
			for (idx, message) in messages.enumerated() {
				self.delegate.consumerDidReturnMessage(
					message,
					offset: offset + Int64(idx)
				)
			}
			
			guard self.delegate.shouldCommitOffset != nil && self.delegate.shouldCommitOffset!(self._topic, partition: self._partition, offset: offset) else {
				return
			}
			
			let metadata = self.delegate.shouldAttachOffsetMetadata?(topicName, partition: partitionId, offset: offset)
			
			coordinator.commitGroupOffset(groupId: groupId, topics: [topicName: [partitionId: (offset, metadata)]], clientId: self._clientId, callback: {
				self.delegate.offsetDidCommit?(topicName, partition: partitionId, offset: offset)
			})
		}, errorCallback: { error in
			switch error {
			case .fetchFailed, .noConnection, .noGroupMembershipForBroker:
				print("Failed polling with a non-kafka error")
			case .kafkaError(let errorCode):
				if errorCode.retriable, self.delegate.shouldRetryFailedFetch?(self._topic, partition: self._partition, errorId: errorCode.code, errorDescription: errorCode.description) ?? true {
					self.poll()
				}
				self.delegate.fetchDidFail?(self._topic, partition: self._partition, errorId: errorCode.code, errorDescription: errorCode.description)
			}
		})
	}
}
