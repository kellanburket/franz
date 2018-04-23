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

class Broker: KafkaType {
	
    var groupMembership = [String: GroupMembership]()

    var nodeId: Int32

	private let connectionConfig: Connection.Config
	lazy private var connection: Connection = {
		do {
			return try Connection(config: connectionConfig)
		} catch Connection.AuthenticationError.authenticationFailed {
			fatalError("Authentication failed")
		} catch Connection.AuthenticationError.unsupportedMechanism(let supportedMechanisms) {
			fatalError("Unsupported mechanism. Supported mechanisms for this broker are \(supportedMechanisms)")
		} catch {
			fatalError("Failed to connect")
		}
	}()
    
    var host: String {
        return connectionConfig.host
    }
	var port: Int32 {
		return connectionConfig.port
	}
    
	init(connectionConfig: Connection.Config) {
		self.connectionConfig = connectionConfig
        nodeId = -1
    }
	
	required init(data: inout Data) {
        nodeId = Int32(data: &data)
        let host = String(data: &data)
        let port = Int32(data: &data)
		//TODO: ??? get connection config in
		connectionConfig = Connection.Config(host: host, port: port, clientId: "placeholder", authentication: .none)
    }
    
    var dataLength: Int {
        return nodeId.dataLength + host.dataLength + port.dataLength
    }
    
    var data: Data {
        return nodeId.data + host.data + port.data
    }
	
	class CancelToken {
		fileprivate var shouldCancel = false
		
		func cancel() {
			shouldCancel = true
		}
	}

	func poll(topics: [TopicName: [PartitionId]], fromStart: Bool, groupId: String, replicaId: ReplicaId, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) -> CancelToken {
		
		let cancelToken = CancelToken()
		
        guard groupMembership[groupId] != nil else {
			errorCallback?(.noGroupMembershipForBroker)
			return cancelToken
		}
		getOffsets(for: topics, time: fromStart ? .earliest : .latest) { offsets in
			let topicsWithOffsets = offsets.mapValues({ partitions in
				partitions.mapValues({ offsets in
					offsets.sorted().last
				})
				.filter({ $1 != nil })
				.mapValues({ $0! })
			})
			self.poll(topics: topicsWithOffsets, replicaId: replicaId, cancelToken: cancelToken, callback: callback, errorCallback: errorCallback)
		}
		return cancelToken
    }
	
	func poll(topics: [TopicName: [PartitionId: Offset]], replicaId: ReplicaId, cancelToken: CancelToken? = nil, callback: @escaping (TopicName, PartitionId, Offset, [Message]) -> (), errorCallback: ((BrokerError) -> Void)? = nil) {
		
		let request = FetchRequest(topics: topics, replicaId: replicaId)
		
		connection.write(request) { response in
			//Check to see if we should stop polling
			if let token = cancelToken, token.shouldCancel {
				return
			}
			
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
			self.poll(topics: topicsWithNewOffsets, replicaId: replicaId, cancelToken: cancelToken, callback: callback, errorCallback: errorCallback)
		}
    }
	
	func fetch(_ topic: TopicName, partition: PartitionId, callback: @escaping ([Message]) -> ()) {
		fetch(topic, partition: partition, offset: 0, replicaId: ReplicaId.none, callback: callback)
	}

    func fetch(_ topic: TopicName, partition: PartitionId, offset: Offset, replicaId: ReplicaId, callback: @escaping ([Message]) -> ()) {
		fetch(topics: [topic: [partition: offset]], replicaId: replicaId, callback: callback)
    }
    
    func fetch(topics: [TopicName: [PartitionId: Offset]], replicaId: ReplicaId, callback: @escaping ([Message]) -> ()) {
        let readQueue = getReadQueue("topics", partition: 0)
		
		let request = FetchRequest(topics: topics, replicaId: replicaId)
        
		connection.write(request) { response in
			readQueue.async {
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
		connection.write(request) { _ in }
    }
	
	func commitGroupOffset(groupId: String, topics: [TopicName: [PartitionId: (Offset, OffsetMetadata?)]], callback: (() -> Void)? = nil) {
		guard let groupMembership = self.groupMembership[groupId] else { return }
		
		let request = OffsetCommitRequest(consumerGroupId: groupId, generationId: groupMembership.group.generationId, consumerId: groupMembership.memberId, topics: topics)
		
		connection.write(request) { response in
			self._metadataReadQueue.async {
				for responseTopic in response.topics {
					for responsePartition in responseTopic.partitions {
						guard let error = responsePartition.error else {
							print("Unable to parse error")
							return
						}
						switch error {
						case .noError:
							callback?()
						case .rebalanceInProgressCode:
							print("Rebalance in progress, retrying offset commit after 1 second")
							self._metadataReadQueue.asyncAfter(deadline: .now() + 1) {
								self.commitGroupOffset(groupId: groupId, topics: topics)
							}
							return
						default:
							print("Error with offset commit \(error)")
						}
					}
				}
			}
		}
	}
    
    func fetchOffsets(groupId: String, topics: [TopicName: [PartitionId]], callback: @escaping ([TopicName: [PartitionId: Offset]]) -> ()) {
		guard self.groupMembership.keys.contains(groupId) else {
			return
		}
		let request = OffsetFetchRequest(
			consumerGroupId: groupId,
			topics: topics
		)

		connection.write(request) { response in
			self._metadataReadQueue.async {
				
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

	func getOffsets(for topics: [TopicName: [PartitionId]], time: TimeOffset = .latest, callback: @escaping ([TopicName: [PartitionId: [Offset]]]) -> ()) {
		let request = OffsetRequest(topics: topics, time: time)
		connection.write(request) { response in
			self._metadataReadQueue.async {
				
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
	
	func getGroupCoordinator(groupId: String, callback: @escaping (GroupCoordinatorResponse) -> Void) {
		let request = GroupCoordinatorRequest(id: groupId)
		connection.write(request, callback: callback)
	}
    
    func listGroups(callback: ((String, String) -> ())? = nil) {
        let listGroupsRequest = ListGroupsRequest()

		connection.write(listGroupsRequest) { response in
			self._metadataReadQueue.async {
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
		connection.write(describeGroupRequest) { response in
			self._metadataReadQueue.async {
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
    
    func join(groupId: String, subscription: [String], callback: ((GroupMembership) -> ())? = nil) {
        let metadata = ConsumerGroupMetadata(subscription: subscription)
        
        let request = GroupMembershipRequest<ConsumerGroupMetadata>(
            groupId: groupId,
            metadata: [AssignmentStrategy.RoundRobin: metadata]
        )
        
		connection.write(request) { response in
			self._groupCoordinationQueue.async {
				guard let error = response.error else {
					print("Unable to parse error.")
					return
				}
				guard error.code == 0 else {
					print("ERROR: \(error.description)")
					return
				}
				let group = Group(
					broker: self,
					clientId: self.connectionConfig.clientId,
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
			}
		}
    }
	
	/// Use this if the client is not the leader
	func syncGroup(_ groupId: String, generationId: Int32, callback: ((GroupMemberAssignment) -> ())? = nil) {
		let request = SyncGroupRequest<GroupMemberAssignment>(groupId: groupId, generationId: generationId, memberId: nil, groupAssignment: [:])
		connection.write(request) { response in
			self._groupCoordinationQueue.async {
				guard let error = response.error else {
					fatalError("Unable to parse error")
				}
				switch error {
				case .noError:
					callback?(response.memberAssignment)
				default:
					print("ERROR: \(error.description)")
				}
			}
		}
	}
	
	/// Use this if the client is currently the leader
    func syncGroup(
        _ groupId: String,
        generationId: Int32,
        memberId: String,
        topics: [TopicName: [PartitionId]],
        userData: Data,
        version: ApiVersion =  0,
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

		connection.write(request) { response in
			self._groupCoordinationQueue.async {
				if let error = response.error {
					if error.code == 0 {
						callback?(response.memberAssignment)
					} else {
						print("ERROR: \(error.description)")
					}
				} else {
					fatalError("Unable to parse error.")
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
        
		connection.write(request) { response in
			self._groupCoordinationQueue.async {
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
        callback: (() -> ())? = nil
    ) {
        let request = HeartbeatRequest(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId
        )
        
        connection.write(request) { response in
            self._groupCoordinationQueue.async {
                //print(response.description)
                if let error = response.error {
                    switch error {
                    case .noError:
                        callback?()
                    default:
                        print("ERROR: \(error.description)")
                    }
                } else {
                    print("Unable to process error.")
                }
            }
        }
    }
	
	func getTopicMetadata(topics: [TopicName] = [], completion: @escaping (MetadataResponse) -> Void) {
		let topicMetadataRequest = TopicMetadataRequest(topics: topics)
		
		connection.write(topicMetadataRequest, callback: completion)
	}
	
	func createTopic(topics: [TopicName: (numPartitions: Int32, replicationFactor: Int16)]) {
		let topicRequests = topics.map { (name, options) in
			CreateTopicsRequest.CreateTopicRequest(topic: name,
												   numPartitions: options.numPartitions,
												   replicationFactor: options.replicationFactor)
		}
		let request = CreateTopicsRequest(requests: topicRequests)
		_ = connection.writeBlocking(request)
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
	
	//MARK: Queues
	
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
	
}
