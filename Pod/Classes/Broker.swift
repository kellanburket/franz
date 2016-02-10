//
//  Broker.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

public enum BrokerError: ErrorType {
    case NoConnection
    case NoGroupMembershipForBroker
}


class Broker: KafkaClass {
    var groupMembership = [String: GroupMembership]()

    private var _nodeId: KafkaInt32
    private var _host: KafkaString
    private var _port: KafkaInt32
    
    private var _readQueues = [Int32: dispatch_queue_t]()
    
    private lazy var _metadataReadQueue = {
        return dispatch_queue_create(
            "metadata.read.stream.franz",
            DISPATCH_QUEUE_SERIAL
        )
    }()

    private lazy var _metadataWriteQueue = {
        return dispatch_queue_create(
            "metadata.write.stream.franz",
            DISPATCH_QUEUE_SERIAL
        )
    }()

    private lazy var _adminReadQueue = {
        return dispatch_queue_create(
            "admin.read.stream.franz",
            DISPATCH_QUEUE_SERIAL
        )
    }()

    private lazy var _groupCoordinationQueue = {
        return dispatch_queue_create(
            "group.read.stream.franz",
            DISPATCH_QUEUE_SERIAL
        )
    }()

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
        return _host.value ?? String()
    }
    
    var ipv4: String {
        return _host.value ?? String()
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
                broker: self,
                clientId: clientId
            )
        }
        
        return _connection!
    }

    func poll(
        topic: String,
        partition: Int32,
        groupId: String,
        clientId: String,
        replicaId: ReplicaId,
        _ callback: (Int64, [Message]) -> (),
        _ errorCallback: (KafkaErrorCode -> ())? = nil
    ) throws {
        if let _ = groupMembership[groupId] {
            fetchGroupOffset(
                groupId,
                topic: topic,
                partition: partition,
                clientId: clientId
            ) { offset in
                self.poll(
                    topic,
                    partition: partition,
                    offset: offset,
                    clientId: clientId,
                    replicaId: replicaId,
                    callback,
                    errorCallback
                )
            }
        } else {
            throw BrokerError.NoGroupMembershipForBroker
        }
    }
    
    func poll(
        topic: String,
        partition: Int32,
        offset: Int64,
        clientId: String,
        replicaId: ReplicaId,
        _ callback: (Int64, [Message]) -> (),
        _ errorCallback: (KafkaErrorCode -> ())? = nil
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
                for responseTopic in response.topics {
                    for responsePartition in responseTopic.partitions {
                        if let error = responsePartition.error {
                            if error.code == 0 {
                                callback(
                                    responsePartition.offset,
                                    responsePartition.messages
                                )
                                
                                self.poll(
                                    topic,
                                    partition: responsePartition.partition,
                                    offset: responsePartition.offset,
                                    clientId: clientId,
                                    replicaId: replicaId,
                                    callback,
                                    errorCallback
                                )
                            } else {
                                if let errorCallback = errorCallback {
                                    errorCallback(error)
                                }
                            }
                        } else {
                            print("Unable to parse error.")
                            print(response.description)
                        }
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
                for responseTopic in response.topics {
                    for responsePartition in responseTopic.partitions {
                        if let error = responsePartition.error {
                            if error.code == 0 {
                                callback(responsePartition.messages)
                            } else {
                                print("ERROR: \(error.description)")
                                print(response.description)
                            }
                        } else {
                            print("Unable to parse error.")
                            print(response.description)
                        }
                    }
                }
            }
        }
    }
    
    func send(topic: String, partition: Int32, batch: MessageSet, clientId: String) {
        let request = ProduceRequest(values: [topic: [partition: batch]])
        connect(clientId).write(request)
    }
    
    func commitGroupOffset(
        groupId: String,
        topic: String,
        partition: Int32,
        offset: Int64,
        metadata: String?,
        clientId: String,
        _ callback: (() -> ())? = nil,
        _ errorCallback: (KafkaErrorCode -> ())? = nil
    ) {
        if let groupMembership = self.groupMembership[groupId] {
            let request = OffsetCommitRequest(
                consumerGroupId: groupId,
                generationId: groupMembership.group.generationId,
                consumerId: groupMembership.memberId,
                topics: [topic: [partition: (offset, metadata)]]
            )
            
            connect(clientId).write(request) { bytes in
                dispatch_async(self._metadataReadQueue) {
                    var mutableBytes = bytes
                    let response = OffsetCommitResponse(bytes: &mutableBytes)
                    for responseTopic in response.topics {
                        for responsePartition in responseTopic.partitions {
                            if let error = responsePartition.error {
                                if error.code != 0 {
                                    errorCallback?(error)
                                } else {
                                    callback?()
                                }
                            } else {
                                print("Unable to parse error.")
                                print(response.description)
                            }
                        }
                    }
                }
            }
        }
    }
    
    func fetchGroupOffset(
        groupId: String,
        topic: String,
        partition: Int32,
        clientId: String,
        callback: (Int64) -> ()
    ) {
        if let _ = self.groupMembership[groupId] {
            let request = OffsetFetchRequest(
                consumerGroupId: groupId,
                topics: [topic: [partition]]
            )

            connect(clientId).write(request) { bytes in
                dispatch_async(self._metadataReadQueue) {
                    var mutableBytes = bytes
                    let response = OffsetFetchResponse(bytes: &mutableBytes)
                    for responseTopic in response.topics {
                        for responsePartition in responseTopic.partitions {
                            if let error = responsePartition.error {
                                if error.code == 0 {
                                    callback(responsePartition.offset)
                                } else {
                                    print("ERROR: \(error.description)")
                                    print(response.description)
                                }
                            } else {
                                print("Unable to parse error.")
                                print(response.description)
                            }
                        }
                    }
                }
            }
        }
    }

    func getOffsets(
        topic: String,
        partition: Int32,
        clientId: String,
        callback: [Int64] -> ()
    ) {
        let request = OffsetRequest(topic: topic, partitions: [partition])
        connect(clientId).write(request) { bytes in
            dispatch_async(self._metadataReadQueue) {
                var mutableBytes = bytes
                let response = OffsetResponse(bytes: &mutableBytes)
                for topicalPartitionedOffsets in response.topicalPartitionedOffsets {
                    for (_, partitionedOffsets) in topicalPartitionedOffsets.partitionedOffsets {
                        if let error = partitionedOffsets.error {
                            if error.code == 0 {
                                callback(partitionedOffsets.offsets)
                            } else {
                                print("ERROR: \(error.description)")
                                print(response.description)
                            }
                        } else {
                            print("Unable to parse error.")
                            print(response.description)
                        }
                    }
                }
            }
        }
    }
    
    func listGroups(clientId: String, callback: ((String, String) -> ())? = nil) {
        let listGroupsRequest = ListGroupsRequest()

        connect(clientId).write(listGroupsRequest) { bytes in
            dispatch_async(self._metadataReadQueue) {
                var mutableBytes = bytes
                let response = ListGroupsResponse(bytes: &mutableBytes)
                if let error = response.error {
                    if error.code == 0 {
                        for (groupId, groupProtocol) in response.groups {
                            if let listGroupsCallback = callback {
                                listGroupsCallback(groupId, groupProtocol)
                            }
                        }
                    } else {
                        print("ERROR: \(error.description)")
                        print(response.description)
                    }
                } else {
                    print("Unable to parse error.")
                    print(response.description)
                }
            }
        }
    }
    
    func describeGroups(
        groupId: String,
        clientId: String,
        callback: ((String, GroupState) -> ())? = nil
    ) {
        let describeGroupRequest = DescribeGroupsRequest(id: groupId)
        connect(clientId).write(describeGroupRequest) { bytes in
            dispatch_async(self._metadataReadQueue) {
                var mutableBytes = bytes
                let response = DescribeGroupsResponse(bytes: &mutableBytes)
                for groupState in response.states {
                    if let error = groupState.error {
                        if error.code == 0 {
                            if let describeGroupCallback = callback {
                                if let id = groupState.id, state = groupState.state {
                                    describeGroupCallback(id, state)
                                }
                            }
                        } else {
                            print("ERROR: \(error.description)")
                            print(response.description)
                        }
                    } else {
                        print("Unable to parse error.")
                        print(response.description)
                    }
                }
            }
        }
    }
    
    func joinGroup(
        groupId: String,
        subscription: [String],
        clientId: String,
        callback: (GroupMembership -> ())? = nil
    ) {
        let metadata = ConsumerGroupMetadata(subscription: subscription)
        
        let request = GroupMembershipRequest<ConsumerGroupMetadata>(
            groupId: groupId,
            metadata: [AssignmentStrategy.RoundRobin: metadata]
        )
        
        connect(clientId).write(request) { bytes in
            dispatch_async(self._groupCoordinationQueue) {
                var mutableBytes = bytes
                let response = JoinGroupResponse(bytes: &mutableBytes)
                //print(response.description)
                
                if let error = response.error {
                    if error.code == 0 {
                        let group = Group(
                            broker: self,
                            clientId: clientId,
                            groupProtocol: response.groupProtocol,
                            groupId: groupId,
                            generationId: response.generationId
                        )
                        
                        let groupMembership = GroupMembership(
                            group: group,
                            memberId: response.memberId
                        )
                        
                        self.groupMembership[groupId] = groupMembership
                        
                        if let joinGroupCallback = callback {
                            joinGroupCallback(groupMembership)
                        }
                    } else {
                        print("ERROR: \(error.description)")
                        print(response.description)
                    }
                } else {
                    print("Unable to parse error.")
                    print(response.description)
                }
            }
        }
    }
    
    func syncGroup(
        groupId: String,
        generationId: Int32,
        memberId: String,
        topics: [String: [Int32]],
        userData: NSData,
        clientId: String,
        version: ApiVersion =  ApiVersion.DefaultVersion,
        callback: (() -> ())? = nil
    ) {
        let groupAssignmentMetadata = [GroupMemberAssignment(
            topics: topics,
            userData: userData,
            version: version
        )]
        
        let request = SyncGroupRequest<GroupMemberAssignment>(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId,
            groupAssignment: groupAssignmentMetadata
        )
        
        connect(clientId).write(request) { bytes in
            dispatch_async(self._groupCoordinationQueue) {
                var mutableBytes = bytes
                let response = SyncGroupResponse<GroupMemberAssignment>(bytes: &mutableBytes)
                if let error = response.error {
                    if error.code == 0 {
                        if let syncGroupCallback = callback {
                            syncGroupCallback()
                        }
                    } else {
                        print("ERROR: \(error.description)")
                        print(response.description)
                    }
                } else {
                    print("Unable to parse error.")
                    print(response.description)
                }
            }
        }
    }
    
    func leaveGroup(
        groupId: String,
        memberId: String,
        clientId: String,
        callback: (() -> ())? = nil
    ) {
        let request = LeaveGroupRequest(groupId: groupId, memberId: memberId)
        
        connect(clientId).write(request) { bytes in
            dispatch_async(self._groupCoordinationQueue) {
                var mutableBytes = bytes
                let response = LeaveGroupResponse(bytes: &mutableBytes)
                if let error = response.error {
                    if error.code == 0 {
                        if let leaveGroupCallback = callback {
                            leaveGroupCallback()
                        }
                    } else {
                        print("ERROR: \(error.description)")
                        print(response.description)
                    }
                } else {
                    print("Unable to parse error.")
                    print(response.description)
                }
            }
        }
    }
    
    func heartbeatRequest(
        groupId: String,
        generationId: Int32,
        memberId: String,
        clientId: String,
        callback: (() -> ())? = nil
    ) {
        let request = HeartbeatRequest(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId
        )
        
        connect(clientId).write(request) { bytes in
            dispatch_async(self._groupCoordinationQueue) {
                var mutableBytes = bytes
                let response = HeartbeatResponse(bytes: &mutableBytes)
                //print(response.description)
                if let error = response.error {
                    if error.code == 0 {
                        if let heartbeatCallback = callback {
                            heartbeatCallback()
                        }
                    } else {
                        print("ERROR: \(error.description)")
                        print(response.description)
                    }
                } else {
                    print("Unable to process error.")
                    print(response.description)
                }
            }
        }
    }
    
    private func getReadQueue(topic: String, partition: Int32) -> dispatch_queue_t {
        var readQueue: dispatch_queue_t
        if let pq = _readQueues[partition] {
            readQueue = pq
        } else {
            _readQueues[partition] = dispatch_queue_create(
                "\(partition).\(topic).read.stream.franz",
                DISPATCH_QUEUE_CONCURRENT
            )
            readQueue = _readQueues[partition]!
        }
        
        return readQueue
    }
}