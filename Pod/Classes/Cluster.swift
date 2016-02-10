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
public enum ClusterError: ErrorType {
    case NoBrokersAvailable
    case BrokerWithKeyNotInitialized(key: String)
    case LeaderNotFound(topic: String, partition: Int32)
    case NoMatchingBrokerFoundInCluster
    case NoLeaderFoundInResponseData
    case NoPartitionFoundInCluster(partition: Int32)
    case NoTopicFoundInCluster(topic: String)
    case NoBatchForTopicPartition(topic: String, partition: Int32)
}


/*
    The Kafka Cluster
*/
public class Cluster {

    private var _brokers = [String: Broker]()
    private var _batches = [String: [Int32: [MessageSetItem]]]()
    private var _clientId: String
    private var _nodeId: ReplicaId
    
    /*
        User-defined Client ID set at start up.
    */
    public var clientId: String {
        return _clientId
    }

    lazy var dispatchQueue: dispatch_queue_t = {
        return dispatch_queue_create(
            "metadata.cluster.read.stream.franz",
            DISPATCH_QUEUE_SERIAL
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
            self._nodeId = ReplicaId.Node(id: replicaId)
        } else {
            self._nodeId = ReplicaId.None
        }
    }
    
    /**
        Send an unbatched message to a topic-partition
     
        - Parameter topic:
        - Parameter partition:
        - Parameter message:
     */
    public func sendMessage(topic: String, partition: Int32 = 0, message: String) {
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
    public func batchMessage(topic: String, partition: Int32, message: String) {
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
    public func sendBatch(topic: String, partition: Int32) throws {
        if let topicBatch = _batches[topic], partitionBatch = topicBatch[partition] {
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
            throw ClusterError.NoBatchForTopicPartition(topic: topic, partition: partition)
        }
    }

    /**
        List all available topics

        - Parameter callback:
     */
    public func listTopics(callback: [Topic] -> ()) {
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
    public func getOffsets(
        topic: String,
        partition: Int32,
        callback: [Int64] -> ()
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
    public func getMessages(
        topic: String,
        partition: Int32,
        offset: Int64,
        callback: (Message) -> ()
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
    public func listGroups(callback: (String, String) -> ()) {
        for (_, broker) in _brokers {
            dispatch_async(dispatchQueue) {
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
    public func getSimpleConsumer(
        topic: String,
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
    public func getHighLevelConsumer(
        topic: String,
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
                    membership.sync([topic: [partition]], data: NSData()) {
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
        id: String,
        topic: String,
        _ callback: (Broker, GroupMembership) -> (),
        _ error: KafkaErrorCode -> ()
    ) {
        getGroupCoordinator(id) { broker in
            broker.joinGroup(id, subscription: [topic], clientId: self.clientId) { groupMembership in
                callback(broker, groupMembership)
            }
        }
    }
    
    private func findTopicLeader(
        topic: String,
        partition: Int32,
        _ callback: Broker -> (),
        _ error: ErrorType -> ()
        ) {
            var dispatchBlocks = [dispatch_block_t]()
            
            for (_, broker) in _brokers {
                let dispatchBlock = dispatch_block_create(DISPATCH_BLOCK_INHERIT_QOS_CLASS) {
                    let connection = broker.connect(self.clientId)
                    let topicMetadataRequest = TopicMetadataRequest(topic: topic)
                    //print(topicMetadataRequest.description)
                    connection.write(topicMetadataRequest) { bytes in
                        var mutableBytes = bytes
                        let response = MetadataResponse(bytes: &mutableBytes)
                        if let topicObj = response.topics[topic] {
                            if let partitionObj = topicObj.partitions[partition] {
                                if partitionObj.leader == -1 {
                                    error(ClusterError.LeaderNotFound(topic: topic, partition: partition))
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
                                    error(ClusterError.NoLeaderFoundInResponseData)
                                }
                            } else {
                                error(ClusterError.NoPartitionFoundInCluster(partition: partition))
                            }
                        } else {
                            error(ClusterError.NoTopicFoundInCluster(topic: topic))
                        }
                        
                        if dispatchBlocks.count > 0 {
                            dispatch_async(self.dispatchQueue, dispatchBlocks.removeFirst())
                        }
                    }
                }
                dispatchBlocks.append(dispatchBlock)
            }
            
            if dispatchBlocks.count > 0 {
                dispatch_async(dispatchQueue, dispatchBlocks.removeFirst())
            }
    }
    
    private func doAdminRequest(request: KafkaRequest, _ callback: [UInt8] -> ()) {
        var dispatchBlocks = [dispatch_block_t]()
        for (_, broker) in _brokers {
            let dispatchBlock = dispatch_block_create(DISPATCH_BLOCK_INHERIT_QOS_CLASS) {
                let connection = broker.connect(self.clientId)
                connection.write(request, callback: callback)
            }
            dispatchBlocks.append(dispatchBlock)
        }
        
        if dispatchBlocks.count > 0 {
            dispatch_async(dispatchQueue, dispatchBlocks.removeFirst())
        }
    }

    private func getGroupCoordinator(id: String, callback: Broker -> ()) {
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