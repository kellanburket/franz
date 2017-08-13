//
//  Cluster.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

/**
    Cluster-related Errors
*/
public enum ClusterError: Error {
    case noBrokersAvailable
    case brokerWithKeyNotInitialized(key: String)
    case leaderNotFound(topic: String, partition: Int32)
    case noMatchingBrokerFoundInCluster
    case noLeaderFoundInResponseData
    case noPartitionFoundInCluster(partition: Int32)
    case noTopicFoundInCluster(topic: String)
    case noBatchForTopicPartition(topic: String, partition: Int32)
}


/*
    The Kafka Cluster
*/
open class Cluster {

    private var _brokers = [String: Broker]()
    private var _batches = [String: [Int32: [MessageSetItem]]]()
    private var _clientId: String
    private var _nodeId: ReplicaId
    
    /*
        User-defined Client ID set at start up.
    */
    open var clientId: String {
        return _clientId
    }

    lazy var dispatchQueue: DispatchQueue = {
        return DispatchQueue(
            label: "metadata.cluster.read.stream.franz",
            attributes: []
        )
    }()
	
	///Used for swapping in fake brokers during testing
	internal var brokers: [Broker] {
		get {
			return Array(_brokers.values)
		}
		set {
			_brokers = [String: Broker]()
			for broker in newValue {
				_brokers["\(broker.ipv4):\(broker.port)"] = broker
			}
		}
	}
	
    /*
        Initialize brokers
    
        - Parameter brokers:
        - Parameter clientId:
        - Parameter nodeId:
    */
    public init(brokers: [(String, Int32)], clientId: String, nodeId: Int32? = nil) {
        for (ipv4, port) in brokers {
            self._brokers["\(ipv4):\(port)"] = Broker(ipv4: ipv4, port: port)
        }
        self._clientId = clientId

        if let replicaId = nodeId {
            self._nodeId = ReplicaId.node(id: replicaId)
        } else {
            self._nodeId = ReplicaId.none
        }
    }
    
    /**
        Send an unbatched message to a topic-partition
     
        - Parameter topic:
        - Parameter partition:
        - Parameter message:
     */
    open func sendMessage(_ topic: String, partition: Int32 = 0, message: String) {
        findTopicLeader(topic, partition: partition, { leader in
            let messages = MessageSet(values: [MessageSetItem(value: message)])
            leader.send(
                topic,
                partition: partition,
                batch: messages,
                clientId: self.clientId
            )
        }, { error in
            print(error)
        })
    }

    /**
        Queue a message to a particular topic-partition

        - Parameter topic:
        - Parameter partition:
        - Parameter message:
     */
    open func batchMessage(_ topic: String, partition: Int32, message: String) {
        let messageSetItem = MessageSetItem(value: message)
        if var topicPartitions = _batches[topic] {
            if var topicPartition = topicPartitions[partition] {
                topicPartition.append(messageSetItem)
                topicPartitions[partition] = topicPartition
                _batches[topic] = topicPartitions
            } else {
               topicPartitions[partition] = [messageSetItem]
                _batches[topic] = topicPartitions
            }
        } else {
            _batches[topic] = [partition: [messageSetItem]]
        }
    }

    /**
        Send batch of queued messages for topic-partition

        - Parameter topic:
        - Parameter partition;
     
        - Throws ClusterError.NoBatchForTopicPartition
     */
    open func sendBatch(_ topic: String, partition: Int32) throws {
		if let topicBatch = _batches[topic], let partitionBatch = topicBatch[partition] {
            findTopicLeader(topic, partition: partition, { leader in
                leader.send(
                    topic,
                    partition: partition,
                    batch: MessageSet(values: partitionBatch),
                    clientId: self.clientId
                )
                self._batches[topic] = nil
            }, { error in
                print(error)
            })
        } else {
            throw ClusterError.noBatchForTopicPartition(topic: topic, partition: partition)
        }
    }
	
	var targetTopics = Set<String>()
	
	func addTargetTopics(topics: [String]) {
		targetTopics.formUnion(topics)
	}

    /**
        List all available topics

        - Parameter callback:
     */
    open func listTopics(_ callback: @escaping ([Topic]) -> ()) {
		_brokers.first?.value.getTopicMetadata(topics: Array(targetTopics), clientId: clientId) { response in
			var topics = [Topic]()
			for (name, topic) in response.topics {
				var partitions = [PartitionId]()
				for (partition, _) in topic.partitions {
					partitions.append(partition)
				}
				topics.append(Topic(name: name, partitions: partitions))
			}
			callback(topics)
		}
    }

    /**
        Get offsets for topic partition

        - Parameter topic:
        - Parameter partition:
        - Parameter callback:
     */
    open func getOffsets(
        _ topic: String,
        partition: Int32,
        callback: @escaping ([Int64]) -> ()
    ) {
        findTopicLeader(topic, partition: partition, { leader in
			leader.getOffsets(for: topic,
                partition: partition,
                clientId: self.clientId,
                callback: callback
            )
        }, { error in
            print(error)
        })
    }

    /**
        Consume messages starting at a particular topic-partition-offset until last message

        - Parameter topic:
        - Parameter partition:
        - Parameter offset:
        - Parameter callback:
     */
    open func getMessages(
        _ topic: String,
        partition: Int32,
        offset: Int64,
        callback: @escaping (Message) -> ()
    ) {
        findTopicLeader(topic, partition: partition, { leader in
            leader.fetch(
                topic,
                partition: partition,
                offset: offset,
                clientId: self.clientId,
                replicaId: self._nodeId
            ) { messages in
                for message in messages {
                    callback(message)
                }
            }
        }, { error in
            print(error)
        })
    }

    /**
        List available groups

        - Parameter callback:
     */
    open func listGroups(_ callback: @escaping (String, String) -> ()) {
        for (_, broker) in _brokers {
            dispatchQueue.async {
				broker.listGroups(clientId: self.clientId) { a, b in
                    callback(a, b)
                }
            }
        }
    }
 
    /**
        Initialize a Simple Conusmer
     
        - Parameter topic:
        - Parameter partition:
        - Parameter delegate:

        - Returns: an uninitialized Simple Consumer
     */
	@available(*, deprecated, message: "Use getConsumer instead")
    public func getSimpleConsumer(
        _ topic: String,
        partition: Int32,
        delegate: ConsumerDelegate
    ) -> SimpleConsumer {
        let consumer = SimpleConsumer(
            topic: topic,
            partition: partition,
            clientId: clientId,
            delegate: delegate
        )
        
        findTopicLeader(topic, partition: partition, { leader in
            consumer.broker = leader

            consumer.delegate.consumerIsReady(consumer)
        }, { error in
            consumer.delegate.topicPartitionLeaderNotFound?(topic, partition: partition)
        })
        
        return consumer
    }
    
    /**
        Initialize a High Level Consumer
     
        - Parameter topic:
        - Parameter partition:
        - Parameter groupId:
        - Parameter delegate:

        - Returns: an unitialized HighLevelConsumer
     */
	@available(*, deprecated, message: "Use getConsumer instead")
    public func getHighLevelConsumer(
        _ topic: String,
        partition: Int32,
        groupId: String,
        delegate: HighLevelConsumerDelegate
    ) -> HighLevelConsumer {
        let consumer = HighLevelConsumer(
            topic: topic,
            partition: partition,
            clientId: clientId,
            delegate: delegate
        )
        
		joinGroup(id: groupId, topics: [topic], callback: { broker, membership in
			consumer.broker = broker
			consumer.membership = membership
			
			membership.group.getState { groupId, state in
				switch state {
				case .AwaitingSync:
					self.assignRoundRobin(members: membership.members.map { $0.memberId }, topics: [topic]) { assignments in
						membership.sync(assignments[membership.memberId]!, data: Data()) {
							consumer.delegate.consumerIsReady(consumer)
						}
					}
				case .Stable:
					consumer.delegate.consumerIsReady(consumer)
				default:
					print("State of Group is: \(state)")
				}
			}
		}, error: { error in
            consumer.delegate.topicPartitionLeaderNotFound?(topic, partition: partition)
        })
        
        return consumer
    }
	
	public func getConsumer(topics: [TopicName], groupId: String) -> Consumer {
		let consumer = Consumer(cluster: self, groupId: groupId)
		joinGroup(id: groupId, topics: topics, callback: { broker, membership in
			consumer.broker = broker
			consumer.membership = membership
			consumer.joinedGroupSemaphore.signal()
			membership.group.getState { groupId, state in
				if state == GroupState.AwaitingSync {
					self.assignRoundRobin(members: membership.members.map { $0.memberId }, topics: topics) { assignments in
						membership.sync(assignments[membership.memberId]!, data: Data()) {}
					}
				}
			}
		}, error: { error in
			
		})
		return consumer
	}
    
    internal func joinGroup(id: String, topics: [TopicName], callback: @escaping (Broker, GroupMembership) -> (), error: (KafkaErrorCode) -> ()
    ) {
		getGroupCoordinator(groupId: id) { broker in
			broker.join(groupId: id, subscription: topics, clientId: self.clientId) { groupMembership in
                callback(broker, groupMembership)
            }
        }
    }
    
    private func findTopicLeader(
        _ topic: String,
        partition: Int32,
        _ callback: @escaping (Broker) -> (),
        _ error: @escaping (Error) -> ()
		) {
		var dispatchBlocks = [()->()]()
		
		for (_, broker) in _brokers {
			let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
				
				broker.getTopicMetadata(topics: [topic], clientId: self.clientId) { response in
					if let topicObj = response.topics[topic] {
						if let partitionObj = topicObj.partitions[partition] {
							if partitionObj.leader == -1 {
								error(ClusterError.leaderNotFound(topic: topic, partition: partition))
								return
							} else if let leader = response.brokers[partitionObj.leader] {
								if let broker = self._brokers["\(leader.host):\(leader.port)"] {
									broker.nodeId = leader.nodeId
									callback(broker)
									return
								} else {
									self._brokers["\(leader.host):\(leader.port)"] = leader
									callback(leader)
									return
								}
							} else {
								error(ClusterError.noLeaderFoundInResponseData)
							}
						} else {
							error(ClusterError.noPartitionFoundInCluster(partition: partition))
						}
					} else {
						error(ClusterError.noTopicFoundInCluster(topic: topic))
					}
					
					if dispatchBlocks.count > 0 {
						self.dispatchQueue.async(execute: dispatchBlocks.removeFirst())
					}
				}
				
			}
			dispatchBlocks.append(dispatchBlock.perform)
		}
		
		if dispatchBlocks.count > 0 {
			dispatchQueue.async(execute: dispatchBlocks.removeFirst())
		}
	}
	
	func getGroupCoordinator(groupId: String, callback: @escaping (Broker) -> Void) {
		_brokers.first?.value.getGroupCoordinator(groupId: groupId, clientId: clientId) { response in
			let host = response.host
			let port = response.port
			
			if let broker = self._brokers["\(host):\(port)"] {
				callback(broker)
			} else {
				print("Broker: \(host):\(port) Not Found.")
			}
		}
	}
	
	func getParitions(for topics: [TopicName], completion: @escaping ([TopicName: [Partition]]) -> Void) {
		_brokers.first?.value.getTopicMetadata(topics: topics, clientId: clientId) { response in
			let partitions = response.topics.mapValues { $0.partitions.map({ $0.value }) }
			completion(partitions)
		}
	}
}
