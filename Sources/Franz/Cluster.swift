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

    var _brokers = [String: Broker]()
    var _batches = [String: [Int32: [MessageSetItem]]]()
    var _clientId: String
    var _nodeId: ReplicaId
    
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

    /**
        List all available topics

        - Parameter callback:
     */
    open func listTopics(_ callback: @escaping ([Topic]) -> ()) {
        doAdminRequest(TopicMetadataRequest()) { bytes in
            var mutableBytes = bytes
            let response = MetadataResponse(bytes: &mutableBytes)
            var topics = [Topic]()
            for (name, topic) in response.topics {
                var partitions = [Int32]()
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
            leader.getOffsets(
                topic,
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
                broker.listGroups(self.clientId) { a, b in
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
    open func getSimpleConsumer(
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
    open func getHighLevelConsumer(
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
        
        joinGroup(groupId, topic: topic, { broker, membership in
            consumer.broker = broker
            consumer.membership = membership

            membership.group.getState { groupId, state in
                switch state {
                case .AwaitingSync:
                    membership.sync([topic: [partition]], data: Data()) {
                        consumer.delegate.consumerIsReady(consumer)
                    }
                case .Stable:
                    consumer.delegate.consumerIsReady(consumer)
                default:
                    print("State of Group is: \(state)")
                }
            }
        }, { error in
            consumer.delegate.topicPartitionLeaderNotFound?(topic, partition: partition)
        })
        
        return consumer
    }
    
    internal func joinGroup(
        _ id: String,
        topic: String,
        _ callback: @escaping (Broker, GroupMembership) -> (),
        _ error: (KafkaErrorCode) -> ()
    ) {
        getGroupCoordinator(id) { broker in
            broker.joinGroup(id, subscription: [topic], clientId: self.clientId) { groupMembership in
                callback(broker, groupMembership)
            }
        }
    }
    
    func findTopicLeader(
        _ topic: String,
        partition: Int32,
        _ callback: @escaping (Broker) -> (),
        _ error: @escaping (Error) -> ()
        ) {
            var dispatchBlocks = [()->()]()
            
            for (_, broker) in _brokers {
				let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
                    let connection = broker.connect(self.clientId)
                    let topicMetadataRequest = TopicMetadataRequest(topic: topic)
                    //print(topicMetadataRequest.description)
                    connection.write(topicMetadataRequest) { bytes in
                        var mutableBytes = bytes
                        let response = MetadataResponse(bytes: &mutableBytes)
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
    
    func doAdminRequest(_ request: KafkaRequest, _ callback: @escaping ([UInt8]) -> ()) {
        var dispatchBlocks = [()->()]()
        for (_, broker) in _brokers {
			let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
                let connection = broker.connect(self.clientId)
                connection.write(request, callback: callback)
            }
            dispatchBlocks.append(dispatchBlock.perform)
        }
        
        if dispatchBlocks.count > 0 {
            dispatchQueue.async(execute: dispatchBlocks.removeFirst())
        }
    }

    func getGroupCoordinator(_ id: String, callback: @escaping (Broker) -> ()) {
        doAdminRequest(GroupCoordinatorRequest(id: id)) { bytes in
            var mutableBytes = bytes
            let response = GroupCoordinatorResponse(bytes: &mutableBytes)
            //print(response.description)
            
            let host = response.host
            let port = response.port
            
            if let broker = self._brokers["\(host):\(port)"] {
                callback(broker)
            } else {
                print("Broker: \(host):\(port) Not Found.")
            }
        }
    }
}
