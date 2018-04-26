//
//  GroupMembershipAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


struct GroupMembershipRequest<T: KafkaMetadata>: KafkaRequest {
	static var apiKey: ApiKey { return .joinGroupRequest }
	static var apiVersion: ApiVersion { return 0 }
	
    init(groupId: String, metadata: [AssignmentStrategy: T], memberId: String = "", sessionTimeout: Int32 = 6000) {
		self.init(
			value: JoinGroupRequestMessage<T>(
			groupId: groupId,
			sessionTimeout: sessionTimeout,
			memberId: memberId,
			protocolType: T.protocolType,
			groupProtocols: metadata))
    }
	
	let values: [Encodable]
    init(value: JoinGroupRequestMessage<T>) {
		self.values = [value]
    }
	
	struct Response: Decodable {
		let error: KafkaErrorCode
		let generationId: Int32
		let memberId: String
		let leaderId: String
		let members: [Member]
		
		private var _groupProtocol: String
		var groupProtocol: GroupProtocol {
			if _groupProtocol == GroupProtocol.consumer.value {
				return GroupProtocol.consumer
			} else {
				return GroupProtocol.custom(name: _groupProtocol)
			}
		}
		
		struct Member: Decodable {
			let name: String
			let metadata: Data
			
			var memberId: MemberId { return name }
			
			init(name: String, metadata: String) {
				self.name = name
				self.metadata = metadata.data(using: .utf8)!
			}
		}
	}
}


struct JoinGroupRequestMessage<T: KafkaMetadata>: Encodable {
    
    let groupId: String
    private let sessionTimeout: Int32
    private let memberId: String
    let protocolType: String
    private let groupProtocols: [JoinGroupProtocol<T>]
    
    init(groupId: String, sessionTimeout: Int32, memberId: String, protocolType: GroupProtocol, groupProtocols: [AssignmentStrategy: T]) {
        self.groupId = groupId
        self.sessionTimeout = sessionTimeout
        self.memberId = memberId
        self.protocolType = protocolType.value
        var values = [JoinGroupProtocol<T>]()

        for (name, metadata) in groupProtocols {
            values.append(JoinGroupProtocol(name: name.rawValue, metadata: metadata))
        }
            
        self.groupProtocols = values
    }
}

struct JoinGroupProtocol<T: KafkaMetadata>: Encodable {
    let name: String
	let metadata: T
	
	func encode(to encoder: Encoder) throws {
		try name.encode(to: encoder)
		let metadataEncoder = KafkaEncoder()
		try metadata.encode(to: metadataEncoder)
		try metadataEncoder.storage.encode(to: encoder)
	}
}

struct ConsumerGroupMetadata: KafkaMetadata {
    let version: ApiVersion
    let subscription: [String]
    let userData: Data?
    
    static let protocolType: GroupProtocol = .consumer

    init(subscription: [String], userData: Data? = nil, version: ApiVersion = 0) {
        self.version = version
        self.subscription = subscription
		self.userData = userData
    }
}


struct SyncGroupRequest<T: KafkaMetadata>: KafkaRequest {
	static var apiKey: ApiKey { return .syncGroupRequest }
	static var apiVersion: ApiVersion { return 0 }
    
    init(groupId: String, generationId: Int32, memberId: String?, groupAssignment: [String: T]) {
        let request = SyncGroupRequestMessage<T>(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId,
            groupAssignment: groupAssignment
        )
        
        self.init(value: request)
    }
	
	let values: [Encodable]
    init(value: SyncGroupRequestMessage<T>) {
		self.values = [value]
    }
	
	struct Response: Decodable {
		let error: KafkaErrorCode
		let memberAssignment: T
		
		init(from decoder: Decoder) throws {
			error = try KafkaErrorCode(from: decoder)
			let assignmentData = try Data(from: decoder)
			memberAssignment = try KafkaDecoder.decode(T.self, data: assignmentData)
		}
	}
}

struct GroupMemberAssignment: KafkaMetadata {
    let version: Int16
    let partitionAssignment: [PartitionAssignment]
    let userData: Data

	static let protocolType: GroupProtocol = .consumer

    init(topics: [TopicName: [PartitionId]], userData: Data, version: ApiVersion) {
        self.version = version

        var values = [PartitionAssignment]()
        for (topic, partitions) in topics {
            values.append(
                PartitionAssignment(topic: topic, partitions: partitions)
            )
        }
        self.partitionAssignment = values
		self.userData = userData
    }
}

struct PartitionAssignment: Codable {
    let topic: String
    let partitions: [PartitionId]
}

typealias MemberId = String

struct SyncGroupRequestMessage<T: KafkaMetadata>: Encodable {
    let groupId: String
    let generationId: Int32
    let memberId: String?
    let groupAssignment: [GroupAssignment<T>]

    init(groupId: String, generationId: Int32, memberId: String?,groupAssignment: [String: T]) {
        self.groupId = groupId
        self.generationId = generationId
        self.memberId = memberId
		self.groupAssignment = groupAssignment.map { GroupAssignment<T>(memberId: $0, memberAssignment: $1) }
    }
	
	struct GroupAssignment<T: KafkaMetadata>: Encodable {
		let memberId: String
		let memberAssignment: T
		
		func encode(to encoder: Encoder) throws {
			try memberId.encode(to: encoder)
			let assignmentEncoder = KafkaEncoder()
			memberAssignment.encode(to: assignmentEncoder)
			try assignmentEncoder.storage.encode(to: encoder)
		}
	}
}


struct HeartbeatRequest: KafkaRequest {
	typealias Response = KafkaErrorCode
	static let apiKey: ApiKey = .heartbeatRequest 
	static let apiVersion: ApiVersion = 0

    init(groupId: String, generationId: Int32, memberId: String) {
        self.init(value: HeartbeatRequestMessage(groupId: groupId, generationId: generationId, memberId: memberId))
    }
	
	let values: [Encodable]
    init(value: HeartbeatRequestMessage) {
		self.values = [value]
    }
}

struct HeartbeatRequestMessage: Encodable {
    let groupId: String
    let generationId: Int32
    let memberId: String
}

struct LeaveGroupRequest: KafkaRequest {
	typealias Response = KafkaErrorCode
	
	static let apiKey: ApiKey = .leaveGroupRequest
	static let apiVersion: ApiVersion = 0
    
    init(groupId: String, memberId: String) {
        self.init(value: LeaveGroupRequestMessage(groupId: groupId, memberId: memberId))
    }
	
	let values: [Encodable]
    init(value: LeaveGroupRequestMessage) {
		self.values = [value]
    }
}

struct LeaveGroupRequestMessage: Encodable {
    let groupId: String
    let memberId: String
}
