//
//  Broker.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


enum BrokerError: ErrorType {
    case NoConnection
}


class Broker: KafkaClass {
    private var _nodeId: KafkaInt32
    private var _host: KafkaString
    private var _port: KafkaInt32

    private var _readQueues = [Int32: dispatch_queue_t]()
    private var _connection: KafkaConnection?

    var nodeId: Int32 {
        get {
            return _nodeId.value
        }
        set(newNodeId) {
            _nodeId.value = newNodeId
        }
    }
    
    var host: String {
        return _host.value
    }
    
    var ipv4: String {
        return _host.value
    }
    
    var port: Int32 {
        return _port.value
    }
    
    var description: String {
        return "BROKER:\n\t" +
            "NODE ID: \(nodeId)\n\t" +
            "HOST: \(host)\n\t" +
            "PORT: \(port)"
    }
    
    init(ipv4: String, port: Int32) {
        _host = KafkaString(value: ipv4)
        _port = KafkaInt32(value: port)
        _nodeId = KafkaInt32(value: -1)
    }
    
    init(nodeId: Int32, host: String, port: Int32) {
        self._nodeId = KafkaInt32(value: Int32(nodeId))
        self._host = KafkaString(value: host)
        self._port = KafkaInt32(value: Int32(port))
    }
    
    required init(inout bytes: [UInt8]) {
        _nodeId = KafkaInt32(bytes: &bytes)
        _host = KafkaString(bytes: &bytes)
        _port = KafkaInt32(bytes: &bytes)
    }
    
    var length: Int {
        return _nodeId.length + _host.length + _port.length
    }
    
    var data: NSData {
        return NSData()
    }

    func connect(clientId: String) -> KafkaConnection {
        if _connection == nil {
            _connection = KafkaConnection(
                ipv4: ipv4,
                port: port,
                clientId: clientId
            )
        }
        
        return _connection!
    }
    
    func poll(
        topic: String,
        partition: Int32,
        offset: Int64,
        clientId: String,
        replicaId: ReplicaId,
        callback: [Message] -> ()
    ) {
        let readQueue = getReadQueue(topic, partition: partition)
        
        let request = FetchRequest(
            partitions: [topic: [partition: offset]],
            replicaId: replicaId
        )
        
        connect(clientId).write(request) { bytes in
            dispatch_async(readQueue) {
                var mutableBytes = bytes
                let response = FetchResponse(bytes: &mutableBytes)
                callback(response.messages)
                for(topic, partitions) in response.offsets {
                    for(partition, highwaterMarkOffset) in partitions {
                        self.poll(
                            topic,
                            partition: partition,
                            offset: highwaterMarkOffset,
                            clientId: clientId,
                            replicaId: replicaId,
                            callback: callback
                        )
                    }
                }
            }
        }
    }

    func fetch(
        topic: String,
        partition: Int32,
        offset: Int64,
        clientId: String,
        replicaId: ReplicaId,
        callback: [Message] -> ()
    ) {
        fetch(
            [topic: [partition: offset]],
            clientId: clientId,
            replicaId: replicaId,
            callback: callback
        )
    }

    func fetch(
        topic: String,
        partition: Int32,
        clientId: String,
        callback: [Message] -> ()
    ) {
        fetch(
            topic,
            partition: partition,
            offset: 0,
            clientId: clientId,
            replicaId: ReplicaId.None,
            callback: callback
        )
    }
    
    func fetch(
        topics: [String: [Int32:Int64]],
        clientId: String,
        replicaId: ReplicaId,
        callback: [Message] -> ()
    ) {
        let readQueue = getReadQueue("topics", partition: 0)
        
        let request = FetchRequest(
            partitions: topics,
            replicaId: replicaId
        )
        
        connect(clientId).write(request) { bytes in
            dispatch_async(readQueue) {
                var mutableBytes = bytes
                let response = FetchResponse(bytes: &mutableBytes)
                //print(response.description)
                callback(response.messages)
            }
        }
    }
    
    func send(topic: String, partition: Int32, message: String, clientId: String) {
        let request = ProduceRequest(values: [topic: [partition: [message]]])
        connect(clientId).write(request)
    }
    
    func getOffsets(topic: String, partition: Int32, clientId: String, callback: [Int64] -> ()) {
        let request = OffsetRequest(topic: topic, partitions: [partition])
        connect(clientId).write(request) { bytes in
            var mutableBytes = bytes
            let response = OffsetResponse(bytes: &mutableBytes)
            callback(response.getOffsets(topic, partition: partition))
        }
    }
    
    func listGroups(clientId: String, callback: Group -> ()) {
        let listGroupsRequest = ListGroupsRequest()
        
        connect(clientId).write(listGroupsRequest) { bytes in
            //print("Bytes: \(bytes)")
            var mutableBytes = bytes
            let response = ListGroupsResponse(bytes: &mutableBytes)
            //print(response.description)

            for (id, type) in response.groups {
                let group = Group(id: id, type: type, broker: self)
                callback(group)
            }
        }
    }
    
    func joinGroup(groupId: String, subscription: [String], clientId: String) {
        let metadata = ConsumerGroupMetadata(subscription: subscription)
        
        let request = GroupMembershipRequest<ConsumerGroupMetadata>(
            groupId: groupId,
            metadata: ["consumer": metadata]
        )

        print(request.description)
        
        connect(clientId).write(request) { bytes in
            var mutableBytes = bytes
            let response = JoinGroupResponse(bytes: &mutableBytes)
            print(response.description)
        }
    }

    private func getReadQueue(topic: String, partition: Int32) -> dispatch_queue_t {
        var readQueue: dispatch_queue_t
        if let pq = _readQueues[partition] {
            readQueue = pq
        } else {
            _readQueues[partition] = dispatch_queue_create(
                "\(partition).\(topic).read.stream.franz",
                DISPATCH_QUEUE_SERIAL
            )
            readQueue = _readQueues[partition]!
        }
        
        return readQueue
    }
}