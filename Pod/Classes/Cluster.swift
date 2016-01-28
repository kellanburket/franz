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
    private var _clientId: String
    private var _nodeId: ReplicaId
    
    public var clientId: String {
        return _clientId
    }

    lazy var dispatchQueue: dispatch_queue_t = {
        return dispatch_queue_create(
            "metadata.read.stream.franz",
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
                        dispatch_sync(self.dispatchQueue, dispatchBlocks.removeFirst())
                    }
                }
            }
            dispatchBlocks.append(dispatchBlock)
        }

        if dispatchBlocks.count > 0 {
            dispatch_sync(dispatchQueue, dispatchBlocks.removeFirst())
        }
    }
    
    private func doAdminRequest(
        request: KafkaRequest,
        _ callback: [UInt8] -> ()
    ) {
        var dispatchBlocks = [dispatch_block_t]()
        
        for (_, broker) in _brokers {
            let dispatchBlock = dispatch_block_create(DISPATCH_BLOCK_INHERIT_QOS_CLASS) {
                let connection = broker.connect(self.clientId)

                connection.write(request, callback: callback)
            }
            dispatchBlocks.append(dispatchBlock)
        }
        
        if dispatchBlocks.count > 0 {
            dispatch_sync(dispatchQueue, dispatchBlocks.removeFirst())
        }
    }

    public func consumeMessages(
        topics: [String: [Int32: Int64]],
        callback: [Message] -> ()
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
        callback: [Message] -> ()
    ) {
        consumeMessages([topic: [partition: offset]], callback: callback)
    }
    
    public func sendMessage(
        topic: String,
        partition: Int32 = 0,
        message: String
    ) {
        findTopicLeader(topic, partition: partition, { leader in
            //print("Topic Leader for \(topic)(\(partition)) is Broker \(leader.nodeId)")
            leader.send(topic, partition: partition, message: message, clientId: self.clientId)
        }, { error in
            print(error)
        })
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

    public func listGroups(callback: Group -> ()) {
        for (_, broker) in _brokers {
            print("Checking Groups For \(broker.host):\(broker.port)")
            broker.listGroups(self.clientId) { group in
                callback(group)
            }
        }
    }
    
    private func getGroupCoordinator(id: String, callback: Broker -> ()) {
        doAdminRequest(GroupCoordinatorRequest(id: id)) { bytes in
            var mutableBytes = bytes
            let response = GroupCoordinatorResponse(bytes: &mutableBytes)
            print(response.description)
            let host = response.host
            let port = response.port
            let nodeId = response.id
            
            if let broker = self._brokers["\(host):\(port)"] {
                callback(broker)
            } else {
                let broker = Broker(nodeId: nodeId, host: host, port: port)
                self._brokers["\(host):\(port)"] = broker
                callback(broker)
            }
        }
    }
    
    public func joinGroup(id: String, topics: [String]) {
        getGroupCoordinator(id) { broker in
            broker.joinGroup(id, subscription: topics, clientId: self.clientId)
        }
    }
    
}