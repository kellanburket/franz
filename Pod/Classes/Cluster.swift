//
//  Cluster.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


public enum ClusterError: ErrorType {
    case NoBrokersAvailable
    case BrokerWithKeyNotInitialized(key: String)
    case LeaderNotFound(topic: String, partition: Int32)
    case NoMatchingBrokerFoundInCluster
    case NoLeaderFoundInResponseData
    case NoPartitionFoundInCluster(partition: Int32)
    case NoTopicFoundInCluster(topic: String)
}


public class Cluster {

    private var _brokers = [String: Broker]()
    private var _batches = [String: [Int32: [MessageSetItem]]]()
    private var _clientId: String
    private var _nodeId: ReplicaId
    
    public var clientId: String {
        return _clientId
    }

    lazy var dispatchQueue: dispatch_queue_t = {
        return dispatch_queue_create(
            "metadata.cluster.read.stream.franz",
            DISPATCH_QUEUE_SERIAL
        )
    }()
    
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

    public func consumeMessages(
        topics: [String: [Int32: Int64]],
        callback: Message -> ()
    ) {
        for (topic, partitions) in topics {
            for(partition, offset) in partitions {
                findTopicLeader(topic, partition: partition, { leader in
                    leader.poll(
                        topic,
                        partition: partition,
                        offset: offset,
                        clientId: self.clientId,
                        replicaId: self._nodeId,
                        callback: callback
                    )
                }, { error in
                    print(error)
                })
            }
        }
    }
    
    public func consumeMessages(
        topic: String,
        partition: Int32 = 0,
        offset: Int64 = 0,
        callback: Message -> ()
    ) {
        consumeMessages([topic: [partition: offset]], callback: callback)
    }

    public func consumeMessages(
        topic: String,
        partition: Int32,
        groupId: String,
        callback: Message -> ()
    ) {
        getGroupCoordinator(groupId) { coordinator in
            do {
                try coordinator.poll(
                    topic,
                    partition: partition,
                    groupId: groupId,
                    clientId: self.clientId,
                    replicaId: self._nodeId,
                    callback: callback
                )
            } catch {
                print("Unable to find consumer group with id '\(groupId)'")
            }
        }
    }
    
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
    
    public func batchMessage(topic: String, partition: Int32, message: String) {
        let messageSetItem = MessageSetItem(value: message)
        if var topicPartitions = _batches[topic] {
            if var partition = topicPartitions[partition] {
                partition.append(messageSetItem)
            } else {
               topicPartitions[partition] = [messageSetItem]
            }
        } else {
            _batches[topic] = [partition: [messageSetItem]]
        }
    }
    
    public func sendBatch(topic: String, partition: Int32) {
        if let topicBatch = _batches[topic], partitionBatch = topicBatch[partition] {
            findTopicLeader(topic, partition: partition, { leader in
                leader.send(
                    topic,
                    partition: partition,
                    batch: MessageSet(values: partitionBatch),
                    clientId: self.clientId
                )
            }, { error in
                print(error)
            })
        }
    }
    
    public func listTopics(callback: [Topic] -> ()) {
        doAdminRequest(TopicMetadataRequest()) { bytes in
            var mutableBytes = bytes
            let response = MetadataResponse(bytes: &mutableBytes)
            var topics = [Topic]()
            for (_, topic) in response.topics {
                topics.append(topic)
            }
            callback(topics)
        }
    }

    public func getOffsets(
        topic: String,
        partition: Int32,
        callback: [Int64] -> ()
    ) {
        findTopicLeader(topic, partition: partition, { leader in
            leader.getOffsets(topic, partition: partition, clientId: self.clientId, callback: callback)
        }, { error in
            print(error)
        })
    }
    
    public func getMessage(
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

    public func listGroups(callback: (String, String) -> ()) {
        for (_, broker) in _brokers {
            dispatch_async(dispatchQueue) {
                // For Some Reason this request needs to be called twice. Not sure why.
                broker.listGroups(self.clientId) { a, b in
                    callback(a, b)
                }
            }
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
    
    public func joinGroup(
        id: String,
        topics: [String],
        callback: (GroupMembership -> ())? = nil
    ) {
        getGroupCoordinator(id) { broker in
            broker.joinGroup(id, subscription: topics, clientId: self.clientId) { groupMembership in
                callback?(groupMembership)
            }
        }
    }
}