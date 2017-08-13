//
//  Broker.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

enum BrokerError: Error {
    case noConnection
    case noGroupMembershipForBroker
	case fetchFailed
	case kafkaError(KafkaErrorCode)
}
//
//protocol BrokerProtocol {
//
//	//MARK: Offset API
//	func getOffsets(for topic: TopicName, partition: PartitionId, clientId: String, callback: @escaping ([Offset]) -> ())
//	func getGroupCoordinator(groupId: String, clientId: String, callback: @escaping (GroupCoordinatorResponse) -> Void)
//	func commitGroupOffset(groupId: String, topics: [TopicName: [PartitionId: (Offset, OffsetCommitRequest.Metadata?)]], clientId: String, callback: (() -> Void)?)
//
//	//MARK: Groups API
//	func join(groupId: String, subscription: [String], clientId: String, callback: ((GroupMembership) -> ())?)
//	func listGroups(clientId: String, callback: ((String, String) -> ())?)
//	func describeGroups(_ groupId: String, clientId: String, callback: ((String, GroupState) -> ())?)
//	func syncGroup(_ groupId: String, generationId: Int32, memberId: String, topics: [TopicName: [PartitionId]], userData: Data, clientId: String, version: ApiVersion, callback: ((GroupMemberAssignment) -> ())?)
//	func leaveGroup(_ groupId: String, memberId: String, clientId: String, callback: (() -> ())?)
//
//	//MARK: Metadata API
//	func getTopicMetadata(topics: [TopicName], clientId: String, completion: @escaping (MetadataResponse) -> Void)
//
//	//MARK: Consumer API
//	func fetch(_ topic: TopicName, partition: PartitionId, clientId: String, callback: @escaping ([Message]) -> ())
//	func fetch(_ topic: TopicName, partition: PartitionId, offset: Offset, clientId: String, replicaId: ReplicaId, callback: @escaping ([Message]) -> ())
//	func fetch(topics: [TopicName: [PartitionId: Offset]], clientId: String, replicaId: ReplicaId, callback: @escaping ([Message]) -> ())
//
//	func poll(topics: [TopicName: [PartitionId]], groupId: String, clientId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)?)
//	func poll(topics: [TopicName: [PartitionId: Offset]], clientId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)?)
//
//	//MARK: Producer API
//	func send(_ topic: TopicName, partition: PartitionId, batch: MessageSet, clientId: String)
//
//	func heartbeatRequest(_ groupId: String, generationId: Int32, memberId: String, clientId: String, callback: (() -> ())?)
//
//	var nodeId: Int32 { get set }
//	var ipv4: String { get }
//	var port: Int32 { get }
//}
//
//extension BrokerProtocol {
//	func commitGroupOffset(groupId: String, topics: [TopicName: [PartitionId: (Offset, OffsetCommitRequest.Metadata?)]], clientId: String) {
//		commitGroupOffset(groupId: groupId, topics: topics, clientId: clientId, callback: nil)
//	}
//}


class Broker: KafkaClass {
	
    var groupMembership = [String: GroupMembership]()

    private var _nodeId: KafkaInt32
    private var _host: KafkaString
    private var _port: KafkaInt32
    
    private var _readQueues = [Int32: DispatchQueue]()
    
	private lazy var _metadataReadQueue: DispatchQueue = {
        return DispatchQueue(
            label: "metadata.read.stream.franz",
            attributes: []
        )
    }()

    private lazy var _metadataWriteQueue: DispatchQueue = {
        return DispatchQueue(
            label: "metadata.write.stream.franz",
            attributes: []
        )
    }()

    private lazy var _adminReadQueue: DispatchQueue = {
        return DispatchQueue(
            label: "admin.read.stream.franz",
            attributes: []
        )
    }()

    private lazy var _groupCoordinationQueue: DispatchQueue = {
        return DispatchQueue(
            label: "group.read.stream.franz",
            attributes: []
        )
    }()

    private var _connection: Connection?

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
        return _nodeId.data + _host.data + _port.data
    }

    func connect(_ clientId: String) -> Connection {
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

	func poll(topics: [TopicName: [PartitionId]], groupId: String, clientId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) {
        guard groupMembership[groupId] != nil else {
			errorCallback?(.noGroupMembershipForBroker)
			return
		}
		fetchOffsets(groupId: groupId, topics: topics, clientId: clientId) { topicsWithOffsets in
			self.poll(topics: topicsWithOffsets, clientId: clientId, replicaId: replicaId, callback: callback, errorCallback: errorCallback)
		}
    }
	
	func poll(topics: [TopicName: [PartitionId: Offset]], clientId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) {
		
		let request = FetchRequest(topics: topics, replicaId: replicaId)
		
        connect(clientId).write(request) { bytes in
			var mutableBytes = bytes
			let response = FetchResponse(bytes: &mutableBytes)
			
			var topicsWithNewOffsets = topics
			
			for responseTopic in response.topics {
				for responsePartition in responseTopic.partitions {
					
					//Update offset for that partition
					topicsWithNewOffsets[responseTopic.topicName]?[responsePartition.partition] = responsePartition.offset
					
					if let error = responsePartition.error {
						if error.code == 0 {
							callback(
								responseTopic.topicName,
								responsePartition.partition,
								responsePartition.offset,
								responsePartition.messages
							)
						} else {
							errorCallback?(.kafkaError(error))
						}
					} else {
						errorCallback?(.fetchFailed)
					}
				}
			}
			
			//Poll again with new offsets
			self.poll(topics: topicsWithNewOffsets, clientId: clientId, replicaId: replicaId, callback: callback, errorCallback: errorCallback)
        }
    }
	
	func fetch(_ topic: TopicName, partition: PartitionId, clientId: String, callback: @escaping ([Message]) -> ()) {
		fetch(topic, partition: partition, offset: 0, clientId: clientId, replicaId: ReplicaId.none, callback: callback)
	}

    func fetch(_ topic: TopicName, partition: PartitionId, offset: Offset, clientId: String, replicaId: ReplicaId, callback: @escaping ([Message]) -> ()) {
		fetch(topics: [topic: [partition: offset]], clientId: clientId, replicaId: replicaId, callback: callback)
    }
    
    func fetch(topics: [TopicName: [PartitionId: Offset]], clientId: String, replicaId: ReplicaId, callback: @escaping ([Message]) -> ()) {
        let readQueue = getReadQueue("topics", partition: 0)
		
		let request = FetchRequest(topics: topics, replicaId: replicaId)
        
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
    
    func send(_ topic: TopicName, partition: PartitionId, batch: MessageSet, clientId: String) {
        let request = ProduceRequest(values: [topic: [partition: batch]])
        connect(clientId).write(request)
    }
	
	func commitGroupOffset(groupId: String, topics: [TopicName: [PartitionId: (Offset, OffsetCommitRequest.Metadata?)]], clientId: String, callback: (() -> Void)? = nil) {
		guard let groupMembership = self.groupMembership[groupId] else { return }
		
		let request = OffsetCommitRequest(consumerGroupId: groupId, generationId: groupMembership.group.generationId, consumerId: groupMembership.memberId, topics: topics)
		
		connect(clientId).write(request) { bytes in
			self._metadataReadQueue.async {
				var mutableBytes = bytes
				let response = OffsetCommitResponse(bytes: &mutableBytes)
				for responseTopic in response.topics {
					for responsePartition in responseTopic.partitions {
						if let error = responsePartition.error {
							if error.code == 0 {
								callback?()
							} else {
								print("Error with offset commit \(error)")
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
    
    func fetchOffsets(groupId: String, topics: [TopicName: [PartitionId]], clientId: String, callback: @escaping ([TopicName: [PartitionId: Offset]]) -> ()) {
        if let _ = self.groupMembership[groupId] {
            let request = OffsetFetchRequest(
                consumerGroupId: groupId,
                topics: topics
            )

            connect(clientId).write(request) { bytes in
                self._metadataReadQueue.async {
                    var mutableBytes = bytes
                    let response = OffsetFetchResponse(bytes: &mutableBytes)
					
					var offsets = [TopicName: [PartitionId: Offset]]()
					
					for topicPartitions in response.topics {
						var entry = [PartitionId: Offset]()
						for partitionOffsets in topicPartitions.partitions {
							entry[partitionOffsets.partition] = partitionOffsets.offset
						}
						offsets[topicPartitions.topic] = entry
					}
					
					callback(offsets)
                }
            }
        }
    }

    func getOffsets(for topic: TopicName, partition: PartitionId, clientId: String, callback: @escaping ([Offset]) -> ()) {
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
	
	func getGroupCoordinator(groupId: String, clientId: String, callback: @escaping (GroupCoordinatorResponse) -> Void) {
		let request = GroupCoordinatorRequest(id: groupId)
		connect(clientId).write(request) { bytes in
			var mutableBytes = bytes
			callback(GroupCoordinatorResponse(bytes: &mutableBytes))
		}
	}
    
    func listGroups(clientId: String, callback: ((String, String) -> ())? = nil) {
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
    
    func join(groupId: String, subscription: [String], clientId: String, callback: ((GroupMembership) -> ())? = nil) {
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
                            memberId: response.memberId,
                            members: response.members
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
        topics: [TopicName: [PartitionId]],
        userData: Data,
        clientId: String,
        version: ApiVersion =  ApiVersion.defaultVersion,
        callback: ((GroupMemberAssignment) -> ())? = nil
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
                            syncGroupCallback(response.memberAssignment)
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
	
	func getTopicMetadata(topics: [TopicName], clientId: String, completion: @escaping (MetadataResponse) -> Void) {
		let topicMetadataRequest = TopicMetadataRequest(topics: topics)
		
		connect(clientId).write(topicMetadataRequest) { bytes in
			var mutableBytes = bytes
			
			completion(MetadataResponse(bytes: &mutableBytes))
		}
	}
    
    private func getReadQueue(_ topic: String, partition: Int32) -> DispatchQueue {
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
