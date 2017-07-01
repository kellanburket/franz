//
//  Broker.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

public enum BrokerError: Error {
    case noConnection
    case noGroupMembershipForBroker
}


class Broker: KafkaClass {
    var groupMembership = [String: GroupMembership]()

    fileprivate var _nodeId: KafkaInt32
    fileprivate var _host: KafkaString
    fileprivate var _port: KafkaInt32
    
    fileprivate var _readQueues = [Int32: DispatchQueue]()
    
	fileprivate lazy var _metadataReadQueue: DispatchQueue = {
        return DispatchQueue(
            label: "metadata.read.stream.franz",
            attributes: []
        )
    }()

    fileprivate lazy var _metadataWriteQueue: DispatchQueue = {
        return DispatchQueue(
            label: "metadata.write.stream.franz",
            attributes: []
        )
    }()

    fileprivate lazy var _adminReadQueue: DispatchQueue = {
        return DispatchQueue(
            label: "admin.read.stream.franz",
            attributes: []
        )
    }()

    fileprivate lazy var _groupCoordinationQueue: DispatchQueue = {
        return DispatchQueue(
            label: "group.read.stream.franz",
            attributes: []
        )
    }()

    fileprivate var _connection: KafkaConnection?

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
        self._nodeId = KafkaInt32(value: nodeId)
        self._host = KafkaString(value: host)
        self._port = KafkaInt32(value: port)
    }
    
	required init( bytes: inout [UInt8]) {
        _nodeId = KafkaInt32(bytes: &bytes)
        _host = KafkaString(bytes: &bytes)
        _port = KafkaInt32(bytes: &bytes)
    }
    
    var length: Int {
        return _nodeId.length + _host.length + _port.length
    }
    
    var data: Data {
        return Data()
    }

    func connect(_ clientId: String) -> KafkaConnection {
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
        _ topic: String,
        partition: Int32,
        groupId: String,
        clientId: String,
        replicaId: ReplicaId,
        _ callback: @escaping (Int64, [Message]) -> (),
        _ errorCallback: ((KafkaErrorCode) -> ())? = nil
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
            throw BrokerError.noGroupMembershipForBroker
        }
    }
    
    func poll(
        _ topic: String,
        partition: Int32,
        offset: Int64,
        clientId: String,
        replicaId: ReplicaId,
        _ callback: @escaping (Int64, [Message]) -> (),
        _ errorCallback: ((KafkaErrorCode) -> ())? = nil
    ) {
        let readQueue = getReadQueue(topic, partition: partition)
        
        let request = FetchRequest(
            partitions: [topic: [partition: offset]],
            replicaId: replicaId
        )
        
        connect(clientId).write(request) { bytes in
            readQueue.async {
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
        _ topic: String,
        partition: Int32,
        offset: Int64,
        clientId: String,
        replicaId: ReplicaId,
        callback: @escaping ([Message]) -> ()
    ) {
        fetch(
            [topic: [partition: offset]],
            clientId: clientId,
            replicaId: replicaId,
            callback: callback
        )
    }

    func fetch(
        _ topic: String,
        partition: Int32,
        clientId: String,
        callback: @escaping ([Message]) -> ()
    ) {
        fetch(
            topic,
            partition: partition,
            offset: 0,
            clientId: clientId,
            replicaId: ReplicaId.none,
            callback: callback
        )
    }
    
    func fetch(
        _ topics: [String: [Int32:Int64]],
        clientId: String,
        replicaId: ReplicaId,
        callback: @escaping ([Message]) -> ()
    ) {
        let readQueue = getReadQueue("topics", partition: 0)
        
        let request = FetchRequest(
            partitions: topics,
            replicaId: replicaId
        )
        
        connect(clientId).write(request) { bytes in
            readQueue.async {
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
    
    func send(_ topic: String, partition: Int32, batch: MessageSet, clientId: String) {
        let request = ProduceRequest(values: [topic: [partition: batch]])
        connect(clientId).write(request)
    }
    
    func commitGroupOffset(
        _ groupId: String,
        topic: String,
        partition: Int32,
        offset: Int64,
        metadata: String?,
        clientId: String,
        _ callback: (() -> ())? = nil,
        _ errorCallback: ((KafkaErrorCode) -> ())? = nil
    ) {
        if let groupMembership = self.groupMembership[groupId] {
            let request = OffsetCommitRequest(
                consumerGroupId: groupId,
                generationId: groupMembership.group.generationId,
                consumerId: groupMembership.memberId,
                topics: [topic: [partition: (offset, metadata)]]
            )
            
            connect(clientId).write(request) { bytes in
                self._metadataReadQueue.async {
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
        _ groupId: String,
        topic: String,
        partition: Int32,
        clientId: String,
        callback: @escaping (Int64) -> ()
    ) {
        if let _ = self.groupMembership[groupId] {
            let request = OffsetFetchRequest(
                consumerGroupId: groupId,
                topics: [topic: [partition]]
            )

            connect(clientId).write(request) { bytes in
                self._metadataReadQueue.async {
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
        _ topic: String,
        partition: Int32,
        clientId: String,
        callback: @escaping ([Int64]) -> ()
    ) {
        let request = OffsetRequest(topic: topic, partitions: [partition])
        connect(clientId).write(request) { bytes in
            self._metadataReadQueue.async {
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
    
    func listGroups(_ clientId: String, callback: ((String, String) -> ())? = nil) {
        let listGroupsRequest = ListGroupsRequest()

        connect(clientId).write(listGroupsRequest) { bytes in
            self._metadataReadQueue.async {
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
        _ groupId: String,
        clientId: String,
        callback: ((String, GroupState) -> ())? = nil
    ) {
        let describeGroupRequest = DescribeGroupsRequest(id: groupId)
        connect(clientId).write(describeGroupRequest) { bytes in
            self._metadataReadQueue.async {
                var mutableBytes = bytes
                let response = DescribeGroupsResponse(bytes: &mutableBytes)
                for groupState in response.states {
                    if let error = groupState.error {
                        if error.code == 0 {
                            if let describeGroupCallback = callback {
								if let id = groupState.id, let state = groupState.state {
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
        _ groupId: String,
        subscription: [String],
        clientId: String,
        callback: ((GroupMembership) -> ())? = nil
    ) {
        let metadata = ConsumerGroupMetadata(subscription: subscription)
        
        let request = GroupMembershipRequest<ConsumerGroupMetadata>(
            groupId: groupId,
            metadata: [AssignmentStrategy.RoundRobin: metadata]
        )
        
        connect(clientId).write(request) { bytes in
            self._groupCoordinationQueue.async {
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
        _ groupId: String,
        generationId: Int32,
        memberId: String,
        topics: [String: [Int32]],
        userData: Data,
        clientId: String,
        version: ApiVersion =  ApiVersion.defaultVersion,
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
            self._groupCoordinationQueue.async {
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
        _ groupId: String,
        memberId: String,
        clientId: String,
        callback: (() -> ())? = nil
    ) {
        let request = LeaveGroupRequest(groupId: groupId, memberId: memberId)
        
        connect(clientId).write(request) { bytes in
            self._groupCoordinationQueue.async {
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
        _ groupId: String,
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
            self._groupCoordinationQueue.async {
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
    
    fileprivate func getReadQueue(_ topic: String, partition: Int32) -> DispatchQueue {
        var readQueue: DispatchQueue
        if let pq = _readQueues[partition] {
            readQueue = pq
        } else {
            _readQueues[partition] = DispatchQueue(
                label: "\(partition).\(topic).read.stream.franz",
                attributes: DispatchQueue.Attributes.concurrent
            )
            readQueue = _readQueues[partition]!
        }
        
        return readQueue
    }
}
