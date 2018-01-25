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


class Broker: KafkaType {
	
    var groupMembership = [String: GroupMembership]()

    var nodeId: Int32
    var host: String
    var port: Int32
    
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
    
    var ipv4: String {
        return host
    }
    
    init(ipv4: String, port: Int32) {
        host = ipv4
        self.port = port
        nodeId = -1
    }
    
    init(nodeId: Int32, host: String, port: Int32) {
        self.nodeId = nodeId
        self.host = host
        self.port = port
    }
    
	required init(data: inout Data) {
        nodeId = Int32(data: &data)
        host = String(data: &data)
        port = Int32(data: &data)
    }
    
    var dataLength: Int {
        return nodeId.dataLength + host.dataLength + port.dataLength
    }
    
    var data: Data {
        return nodeId.data + host.data + port.data
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
	
	class CancelToken {
		fileprivate var shouldCancel = false
		
		func cancel() {
			shouldCancel = true
		}
	}

	func poll(topics: [TopicName: [PartitionId]], fromStart: Bool, groupId: String, clientId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) -> CancelToken {
		
		let cancelToken = CancelToken()
		
        guard groupMembership[groupId] != nil else {
			errorCallback?(.noGroupMembershipForBroker)
			return cancelToken
		}
		getOffsets(for: topics, clientId: clientId, time: fromStart ? .earliest : .latest) { offsets in
			let topicsWithOffsets = offsets.mapValues({ partitions in
				partitions.mapValues({ offsets in
					offsets.sorted().last
				})
				.filter({ $1 != nil })
				.mapValues({ $0! })
			})
			self.poll(topics: topicsWithOffsets, clientId: clientId, replicaId: replicaId, cancelToken: cancelToken, callback: callback, errorCallback: errorCallback)
		}
		return cancelToken
    }
	
	func poll(topics: [TopicName: [PartitionId: Offset]], clientId: String, replicaId: ReplicaId, cancelToken: CancelToken? = nil, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) {
		
		let request = FetchRequest(topics: topics, replicaId: replicaId)
		
        connect(clientId).write(request) { data in
			//Check to see if we should stop polling
			if let token = cancelToken, token.shouldCancel {
				return
			}
			
			var mutableData = data
			let response = FetchResponse(data: &mutableData)
			
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
			self.poll(topics: topicsWithNewOffsets, clientId: clientId, replicaId: replicaId, cancelToken: cancelToken, callback: callback, errorCallback: errorCallback)
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
        
        connect(clientId).write(request) { data in
            readQueue.async {
                var mutableData = data
                let response = FetchResponse(data: &mutableData)
                for responseTopic in response.topics {
                    for responsePartition in responseTopic.partitions {
                        if let error = responsePartition.error {
                            if error.code == 0 {
                                callback(responsePartition.messages)
                            } else {
                                print("ERROR: \(error.description)")
                            }
                        } else {
                            print("Unable to parse error.")
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
	
	func commitGroupOffset(groupId: String, topics: [TopicName: [PartitionId: (Offset, OffsetMetadata?)]], clientId: String, callback: (() -> Void)? = nil) {
		guard let groupMembership = self.groupMembership[groupId] else { return }
		
		let request = OffsetCommitRequest(consumerGroupId: groupId, generationId: groupMembership.group.generationId, consumerId: groupMembership.memberId, topics: topics)
		
		connect(clientId).write(request) { data in
			self._metadataReadQueue.async {
				var mutableData = data
				let response = OffsetCommitResponse(data: &mutableData)
				for responseTopic in response.topics {
					for responsePartition in responseTopic.partitions {
						if let error = responsePartition.error {
							if error.code == 0 {
								callback?()
							}
							else {
								print("Error with offset commit \(error)")
							}
						} else {
							print("Unable to parse error.")
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

            connect(clientId).write(request) { data in
                self._metadataReadQueue.async {
                    var mutableData = data
                    let response = OffsetFetchResponse(data: &mutableData)
					
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

	func getOffsets(for topics: [TopicName: [PartitionId]], clientId: String, time: TimeOffset = .latest, callback: @escaping ([TopicName: [PartitionId: [Offset]]]) -> ()) {
		let request = OffsetRequest(topics: topics, time: time)
        connect(clientId).write(request) { data in
            self._metadataReadQueue.async {
                var mutableData = data
                let response = OffsetResponse(data: &mutableData)
				
				if let error = response.topicalPartitionedOffsets.flatMap({ $0.partitionedOffsets.compactMap { $0.value.error } })
					.filter({ $0 != .noError })
					.first {
					print("ERROR: \(error.description)")
					return
				}
				
				let topicsWithOffsets = Dictionary(uniqueKeysWithValues: response.topicalPartitionedOffsets.map { topic -> (TopicName, [PartitionId : [Offset]]) in
					let partitions = Dictionary(uniqueKeysWithValues: topic.partitionedOffsets.map { (partitionId, offsets) in
						(partitionId, offsets.offsets)
					})
					return (topic.topicName, partitions)
				})
				
				callback(topicsWithOffsets)
            }
        }
    }
	
	func getGroupCoordinator(groupId: String, clientId: String, callback: @escaping (GroupCoordinatorResponse) -> Void) {
		let request = GroupCoordinatorRequest(id: groupId)
		connect(clientId).write(request) { data in
			var mutableData = data
			callback(GroupCoordinatorResponse(data: &mutableData))
		}
	}
    
    func listGroups(clientId: String, callback: ((String, String) -> ())? = nil) {
        let listGroupsRequest = ListGroupsRequest()

        connect(clientId).write(listGroupsRequest) { data in
            self._metadataReadQueue.async {
                var mutableData = data
                let response = ListGroupsResponse(data: &mutableData)
                if let error = response.error {
                    if error.code == 0 {
                        for (groupId, groupProtocol) in response.groups {
                            if let listGroupsCallback = callback {
                                listGroupsCallback(groupId, groupProtocol)
                            }
                        }
                    } else {
                        print("ERROR: \(error.description)")
                    }
                } else {
                    print("Unable to parse error.")
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
        connect(clientId).write(describeGroupRequest) { data in
            self._metadataReadQueue.async {
                var mutableData = data
                let response = DescribeGroupsResponse(data: &mutableData)
                for groupState in response.states {
                    if let error = groupState.error {
                        if error.code == 0 {
                            if let describeGroupCallback = callback, let id = groupState.id {
								describeGroupCallback(id, groupState.state)
                            }
                        } else {
                            print("ERROR: \(error.description)")
                        }
                    } else {
                        print("Unable to parse error.")
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
        
        connect(clientId).write(request) { data in
            self._groupCoordinationQueue.async {
                var mutableData = data
                let response = JoinGroupResponse(data: &mutableData)
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
                    }
                } else {
                    print("Unable to parse error.")
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
        let groupAssignmentMetadata = GroupMemberAssignment(
            topics: topics,
            userData: userData,
            version: version
        )
        
        let request = SyncGroupRequest<GroupMemberAssignment>(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId,
            groupAssignment: [memberId: groupAssignmentMetadata]
		)

        connect(clientId).write(request) { data in
            self._groupCoordinationQueue.async {
                var mutableData = data
                let response = SyncGroupResponse<GroupMemberAssignment>(data: &mutableData)
                if let error = response.error {
                    if error.code == 0 {
                        if let syncGroupCallback = callback {
                            syncGroupCallback(response.memberAssignment)
                        }
                    } else {
                        print("ERROR: \(error.description)")
                    }
                } else {
                    print("Unable to parse error.")
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
        
        connect(clientId).write(request) { data in
            self._groupCoordinationQueue.async {
                var mutableData = data
                let response = LeaveGroupResponse(data: &mutableData)
                if let error = response.error {
                    if error.code == 0 {
                        if let leaveGroupCallback = callback {
                            leaveGroupCallback()
                        }
                    } else {
                        print("ERROR: \(error.description)")
                    }
                } else {
                    print("Unable to parse error.")
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
        
        connect(clientId).write(request) { data in
            self._groupCoordinationQueue.async {
                var mutableData = data
                let response = HeartbeatResponse(data: &mutableData)
                //print(response.description)
                if let error = response.error {
					switch error {
					case .noError:
						callback?()
					//TODO: rejoin the group
					//case .rebalanceInProgressCode:
					default:
						print("ERROR: \(error.description)")
					}
                } else {
                    print("Unable to process error.")
                }
            }
        }
    }
	
	func getTopicMetadata(topics: [TopicName] = [], clientId: String, completion: @escaping (MetadataResponse) -> Void) {
		let topicMetadataRequest = TopicMetadataRequest(topics: topics)
		
		connect(clientId).write(topicMetadataRequest) { data in
			var mutableData = data
			
			completion(MetadataResponse(data: &mutableData))
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
