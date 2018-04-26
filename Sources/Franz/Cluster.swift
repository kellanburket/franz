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


/**
A cluster that represents a connection to multiple brokers.
*/
public class Cluster {

    private var batches = [String: [Int32: [MessageSetItem]]]()
    let clientId: String
	let nodeId: ReplicaId

    lazy var dispatchQueue: DispatchQueue = {
        return DispatchQueue(label: "metadata.cluster.read.stream.franz")
    }()
	
	private var brokers = [String: Broker]()
    
    /**
        Create a new cluster
        - Parameter brokers: The brokers to connect to
        - Parameter clientId: The client ID to represent you
        - Parameter nodeId:
    */
	public init(brokers: [(String, Int32)], clientId: String, nodeId: Int32? = nil, authentication: Authentication = .none) {
        for (ipv4, port) in brokers {
			let connectionConfig = Connection.Config(host: ipv4, port: port, clientId: clientId, authentication: authentication)
			self.brokers["\(ipv4):\(port)"] = Broker(connectionConfig: connectionConfig)
        }
        self.clientId = clientId

        if let replicaId = nodeId {
            self.nodeId = ReplicaId.node(id: replicaId)
        } else {
            self.nodeId = ReplicaId.none
        }
		
		self.authentication = authentication
    }
	
	/// The type of authentication to use when connecting to each broker.
	public enum Authentication {
		
		/// Don't use any authentication
		case none
		
		/// Use PLAIN authentication.
		/// - parameter username: The username
		/// - parameter password: The password
		/// This is currently sent over plain text (PLAINTEXT)
		case plain(username: String, password: String)
		
		internal var mechanism: SaslMechanism? {
			switch self {
			case .plain(let username, let password):
				return PlainMechanism(username: username, password: password)
			default:
				return nil
			}
		}
	}
	let authentication: Authentication
    
    /**
        Send an unbatched message to a topic-partition
     
        - Parameter topic:
        - Parameter partition:
        - Parameter message:
     */
    public func sendMessage(_ topic: String, partition: Int32 = 0, message: String) {
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
    public func batchMessage(_ topic: String, partition: Int32, message: String) {
        let messageSetItem = MessageSetItem(value: message)
        if var topicPartitions = batches[topic] {
            if var topicPartition = topicPartitions[partition] {
                topicPartition.append(messageSetItem)
                topicPartitions[partition] = topicPartition
                batches[topic] = topicPartitions
            } else {
               topicPartitions[partition] = [messageSetItem]
                batches[topic] = topicPartitions
            }
        } else {
            batches[topic] = [partition: [messageSetItem]]
        }
    }

    /**
        Send batch of queued messages for topic-partition

        - Parameter topic:
        - Parameter partition;
     
        - Throws ClusterError.NoBatchForTopicPartition
     */
    public func sendBatch(_ topic: String, partition: Int32) throws {
        if let topicBatch = batches[topic], let partitionBatch = topicBatch[partition] {
            findTopicLeader(topic, partition: partition, { leader in
                leader.send(
                    topic,
                    partition: partition,
                    batch: MessageSet(values: partitionBatch),
                    clientId: self.clientId
                )
                self.batches[topic] = nil
            }, { error in
                print(error)
            })
        } else {
            throw ClusterError.noBatchForTopicPartition(topic: topic, partition: partition)
        }
    }
	
	//MARK: Topics

    /**
        List all available topics

        - Parameter callback:
     */
    public func listTopics(_ callback: @escaping ([Topic]) -> ()) {
		self.brokers.first?.value.getTopicMetadata { response in
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
		Create a new topic
		- parameter name: The name of the topic
		- parameter partitions: The number of partitions to be created
		- parameter replicationFactor: The number of replicas the topic should be stored on
	*/
	public func createTopic(name: String, partitions: Int32, replicationFactor: Int16) {
		dispatchQueue.async {
			self.brokers.first?.value.createTopic(topics: [name: (partitions, replicationFactor)])
		}
	}

    /**
        Get offsets for topic partition

        - Parameter topic:
        - Parameter partition:
        - Parameter callback:
     */
    public func getOffsets(
        _ topic: String,
        partition: Int32,
        callback: @escaping ([Int64]) -> ()
    ) {
        findTopicLeader(topic, partition: partition, { leader in
			leader.getOffsets(for: [topic: [partition]],
                callback: { callback($0[topic]![partition]!) }
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
    public func getMessages(
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
                replicaId: self.nodeId
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
    public func listGroups(_ callback: @escaping (String, String) -> ()) {
        for (_, broker) in brokers {
            dispatchQueue.async {
                broker.listGroups() { a, b in
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
    
    /**
    Initialize a Consumer that can be used to listen for messages.
    
    - parameters:
        - topics: The list of topics that the consumer should be subscribed to.
        - groupId: The id of the consumer group that the consumer group should belong to.
    
    - returns: The Consumer object that can then listen for messages.
    
    - seealso: `Consumer`
    */
    public func getConsumer(topics: [TopicName], groupId: String) -> Consumer {
		let consumer = Consumer(cluster: self, groupId: groupId)
		dispatchQueue.async {
			func joinGroup() {
				self.joinGroup(id: groupId, topics: topics, callback: { broker, membership in
					func checkState() {
						membership.group.getState { groupId, state in
							switch state {
							case .AwaitingSync, .CompletingRebalance:
								//TODO: check if currently the leader and only assign if leader
								self.assignRoundRobin(members: membership.members.map { $0.memberId }, topics: topics) { assignments in
									membership.sync(assignments[membership.memberId]!, data: Data()) {
										consumer.broker = broker
										consumer.membership = membership
										consumer.joinedGroupSemaphore.signal()
									}
								}
							case .Initialize, .PreparingRebalance, .Joining:
								self.dispatchQueue.asyncAfter(deadline: .now() + 1, execute: checkState)
							case .Stable:
								consumer.broker = broker
								consumer.membership = membership
								consumer.joinedGroupSemaphore.signal()
							case .Empty, .Dead, .Down:
								joinGroup()
							}
						}
					}
					checkState()
				}, error: { error in
					
				})
			}
			joinGroup()
		}
        return consumer
    }
    
    internal func joinGroup(id: String, topics: [TopicName], callback: @escaping (Broker, GroupMembership) -> (), error: (KafkaErrorCode) -> ()
    ) {
        getGroupCoordinator(groupId: id) { broker in
            broker.join(groupId: id, subscription: topics) { groupMembership in
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
        
        for (_, broker) in brokers {
            let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
                
                var handleGetTopicMetadata: ((TopicMetadataRequest.Response) -> Void)!
                handleGetTopicMetadata = { response in
                    
                    if let topicObj = response.topics[topic] {
                        
                        if topicObj.error == .leaderNotAvailable {
							let retryTopics = response.topics.filter { key, val in val.error == .leaderNotAvailable }.compactMap { $1.name }
							print("Leader not available, trying to find leader again in 1 second")
                            Thread.sleep(forTimeInterval: 1)
                            broker.getTopicMetadata(topics: retryTopics, completion: handleGetTopicMetadata)
                            return
                        }
                        
                        if let partitionObj = topicObj.partitions[partition] {
                            if partitionObj.leader == -1 {
                                error(ClusterError.leaderNotFound(topic: topic, partition: partition))
                                return
                            } else if let leader = response.brokers[partitionObj.leader] {
                                if let broker = self.brokers["\(leader.host):\(leader.port)"] {
                                    broker.nodeId = leader.nodeId
                                    callback(broker)
                                    return
                                } else {
                                    self.brokers["\(leader.host):\(leader.port)"] = leader
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
                }
                
                broker.getTopicMetadata(topics: [topic], completion: handleGetTopicMetadata)
                
                if dispatchBlocks.count > 0 {
                    self.dispatchQueue.async(execute: dispatchBlocks.removeFirst())
                }
                
            }
            dispatchBlocks.append(dispatchBlock.perform)
        }
        
        if dispatchBlocks.count > 0 {
            dispatchQueue.async(execute: dispatchBlocks.removeFirst())
        }
    }
    
    func getGroupCoordinator(groupId: String, callback: @escaping (Broker) -> Void) {
        brokers.first?.value.getGroupCoordinator(groupId: groupId) { response in
            let host = response.host
            let port = response.port
            
            //Retry if broker isn't available
            if response.error == KafkaErrorCode.groupCoordinatorNotAvailableCode {
                sleep(1)
                self.getGroupCoordinator(groupId: groupId, callback: callback)
                return
            }
            
            if let broker = self.brokers["\(host):\(port)"] {
                callback(broker)
            } else {
                print("Broker: \(host):\(port) Not Found.")
            }
        }
    }
    
    func getParitions(for topics: [TopicName], completion: @escaping ([TopicName: [Partition]]) -> Void) {
        brokers.first?.value.getTopicMetadata(topics: topics) { response in
            let partitions = response.topics.mapValues { $0.partitions.values.map({ $0 }) }
            completion(partitions)
        }
    }
}
