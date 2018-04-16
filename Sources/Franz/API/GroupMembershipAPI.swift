//
//  GroupMembershipAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


struct GroupMembershipRequest<T: KafkaMetadata>: KafkaRequest {

	typealias Response = JoinGroupResponse
	var apiKey: ApiKey { return .joinGroupRequest }
	
    init(
        groupId: String,
        metadata: [AssignmentStrategy: T],
        memberId: String = "",
        sessionTimeout: Int32 = 6000
    ) {
        self.init(
            value: JoinGroupRequestMessage<T>(
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                memberId: memberId,
                protocolType: T.protocolType,
                groupProtocols: metadata
            )
        )
    }
	
	let value: KafkaType?
    init(value: JoinGroupRequestMessage<T>) {
		self.value = value
    }
}


struct JoinGroupRequestMessage<T: KafkaMetadata>: KafkaType {
    
    private var _groupId: String
    private var _sessionTimeout: Int32
    private var _memberId: String
    private var _protocolType: String
    private var _groupProtocols: [JoinGroupProtocol<T>]
    
    init(
        groupId: String,
        sessionTimeout: Int32,
        memberId: String,
        protocolType: GroupProtocol,
        groupProtocols: [AssignmentStrategy: T]
    ) {
        _groupId = groupId
        _sessionTimeout = sessionTimeout
        _memberId = memberId
        _protocolType = protocolType.value
        var values = [JoinGroupProtocol<T>]()

        for (name, metadata) in groupProtocols {
            values.append(JoinGroupProtocol(name: name.rawValue, metadata: metadata))
        }
            
        _groupProtocols = values
    }
    
	init(data: inout Data) {
        _groupId = String(data: &data)
        _sessionTimeout = Int32(data: &data)
        _memberId = String(data: &data)
        _protocolType = String(data: &data)
        _groupProtocols = [JoinGroupProtocol<T>](data: &data)
    }
    
	var dataLength: Int {
        return self._groupId.dataLength +
            self._sessionTimeout.dataLength +
            self._memberId.dataLength +
            self._protocolType.dataLength +
            self._groupProtocols.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._sessionTimeout.data)
        data.append(self._memberId.data)
        data.append(self._protocolType.data)
        data.append(self._groupProtocols.data)
        return data
    }
    
    var groupId: String {
		return _groupId
    }
    
    var protocolType: String {
		return _protocolType
    }
}

struct JoinGroupProtocol<T: KafkaMetadata>: KafkaType {
    private var _protocolName: String
    private var _protocolMetadata: T

    init(name: String, metadata: T) {
        _protocolName = name
        _protocolMetadata = metadata
    }
    
	init(data: inout Data) {
        _protocolName = String(data: &data)
        _protocolMetadata = T(data: &data)
    }
    
    var dataLength: Int {
        return self._protocolName.dataLength +
            self._protocolMetadata.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._protocolName.data)
        data.append(self._protocolMetadata.data)
        return data
    }

}

struct ConsumerGroupMetadata: KafkaMetadata {
    
    private var _version: Int16
    private var _subscription: [String]
    private var _userData: Data?
    
    static var protocolType: GroupProtocol {
        return GroupProtocol.consumer
    }

    init(
        subscription: [String],
        userData: Data? = nil,
        version: ApiVersion = ApiVersion.defaultVersion
    ) {
        _version = version.rawValue
        _subscription = subscription
		_userData = userData
    }
    
    init(data: inout Data) {
        _version = Int16(data: &data)
        _subscription = [String](data: &data)
        _userData = Data(data: &data)
    }
    
    var dataLength: Int {
        return self.sizeDataLength + self.valueDataLength
    }
    
    var sizeDataLength: Int {
        return 4
    }
    
    var valueDataLength: Int {
        return self._version.dataLength +
            self._subscription.dataLength +
            self._userData.dataLength
    }
    
    private var sizeData: Data {
        return (Int32(self.valueDataLength).data)
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self.sizeData)
        data.append(self._version.data)
        data.append(self._subscription.data)
        data.append(self._userData.data)
        return data
    }
}


struct JoinGroupResponse: KafkaResponse {
    
    private var _errorCode: Int16
	
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    private(set) var generationId: Int32
    
    private(set) var memberId: String
    
    private(set) var leaderId: String
	
    private var _groupProtocol: String
    var groupProtocol: GroupProtocol {
        if _groupProtocol == GroupProtocol.consumer.value {
            return GroupProtocol.consumer
        } else {
			return GroupProtocol.custom(name: _groupProtocol)
        }
    }
    
    private(set) var members: [Member]
    
    init(data: inout Data) {
        _errorCode = Int16(data: &data)
        generationId = Int32(data: &data)
        _groupProtocol = String(data: &data)
        leaderId = String(data: &data)
        memberId = String(data: &data)
        members = [Member](data: &data)
    }
	
	var data: Data {
		let values: [KafkaType] = [_errorCode, generationId, _groupProtocol, leaderId, memberId, members]
		return values.map { $0.data }.reduce(Data(), +)
	}
	
	var dataLength: Int {
		let values: [KafkaType] = [_errorCode, generationId, _groupProtocol, leaderId, memberId, members]
		return values.map { $0.dataLength }.reduce(0, +)
	}
}

struct Member: KafkaType {
    private var _memberName: String
    private var _memberMetadata: Data
	
	var memberId: MemberId {
		return _memberName
	}
    
    var name: String {
		return _memberName
    }
    
    init(name: String, metadata: String) {
        _memberName = name
        _memberMetadata = metadata.data(using: .utf8)!
    }
    
    init(data: inout Data) {
        _memberName = String(data: &data)
        _memberMetadata = Data(data: &data)
    }
    
	var dataLength: Int {
        return self._memberName.dataLength + self._memberMetadata.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._memberName.data)
        data.append(self._memberMetadata.data)
        return data
    }
}


struct SyncGroupRequest<T: KafkaMetadata>: KafkaRequest {
	
	typealias Response = SyncGroupResponse<T>
	var apiKey: ApiKey { return .syncGroupRequest }
    
    init(
        groupId: String,
        generationId: Int32,
        memberId: String,
        groupAssignment: [String: T]
    ) {
        let request = SyncGroupRequestMessage<T>(
            groupId: groupId,
            generationId: generationId,
            memberId: memberId,
            groupAssignment: groupAssignment
        )
        
        self.init(value: request)
    }
	
	let value: KafkaType?
    init(value: SyncGroupRequestMessage<T>) {
		self.value = value
    }
}

struct GroupMemberAssignment: KafkaMetadata {
    private var _version: Int16
    let partitionAssignment: [PartitionAssignment]
    private var _userData: Data

    static var protocolType: GroupProtocol {
        return GroupProtocol.consumer
    }

    init(topics: [TopicName: [PartitionId]], userData: Data, version: ApiVersion) {
        _version = version.rawValue

        var values = [PartitionAssignment]()
        for (topic, partitions) in topics {
            values.append(
                PartitionAssignment(topic: topic, partitions: partitions)
            )
        }
        partitionAssignment = values
		_userData = userData
    }

    init(data: inout Data) {
		if data.count <= 0 {
			_version = 0
			partitionAssignment = []
			_userData = Data()
			return
		}
        _version = Int16(data: &data)
        partitionAssignment = [PartitionAssignment](data: &data)
        _userData = Data(data: &data)
    }
    
    var dataLength: Int {
        return self._version.dataLength +
            self.partitionAssignment.dataLength +
            self._userData.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._version.data)
        data.append(self.partitionAssignment.data)
        data.append(self._userData.data)
        return data
    }
}

struct PartitionAssignment: KafkaType {
    let topic: String
    let partitions: [PartitionId]
    
    init(topic: String, partitions: [PartitionId]) {
        self.topic = topic
        self.partitions = partitions
    }

    init(data: inout Data) {
        topic = String(data: &data)
        partitions = [PartitionId](data: &data)
    }
    
    var dataLength: Int {
        return self.topic.dataLength + self.partitions.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self.topic.data)
        data.append(self.partitions.data)
        return data
    }
}

typealias MemberId = String

struct SyncGroupRequestMessage<T: KafkaMetadata>: KafkaType {
    
    private var _groupId: String
    private var _generationId: Int32
    private var _memberId: String
    private var _groupAssignment: [GroupAssignment<T>]

    init(
        groupId: String,
        generationId: Int32,
        memberId: String,
        groupAssignment: [String: T]
    ) {
        _groupId = groupId
        _generationId = generationId
        _memberId = memberId
		_groupAssignment = groupAssignment.map { GroupAssignment<T>(memberId: $0, memberAssignment: $1) }
    }
    
	init(data: inout Data) {
        _groupId = String(data: &data)
        _generationId = Int32(data: &data)
        _memberId = String(data: &data)
        _groupAssignment = [GroupAssignment<T>](data: &data)
    }
    
    var dataLength: Int {
        return self._groupId.dataLength +
            self._generationId.dataLength +
            self._memberId.dataLength +
            self._groupAssignment.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._generationId.data)
        data.append(self._memberId.data)
        data.append(self._groupAssignment.data)
        return data
    }
    
    var groupId: String {
        return _groupId
    }
	
	struct GroupAssignment<T: KafkaMetadata>: KafkaType {
		
		let memberId: String
		let memberAssignment: T
		
		private var memberAssignmentData: Data {
			return Data(memberAssignment.data)
		}
		
		init(data: inout Data) {
			memberId = String(data: &data)
			memberAssignment = T(data: &data)
		}
		
		init(memberId: String, memberAssignment: T) {
			self.memberId = memberId
			self.memberAssignment = memberAssignment
		}
		
		var data: Data {
			return memberId.data + memberAssignmentData.data
		}
		
		var dataLength: Int {
			return memberId.dataLength + memberAssignmentData.dataLength
		}
		
	}
}


struct SyncGroupResponse<T: KafkaMetadata>: KafkaResponse {
    
    private var _errorCode: Int16
    let memberAssignment: T
    
    init(data: inout Data) {
        _errorCode = Int16(data: &data)
		var memberAssignmentData = Data(data: &data)
		memberAssignment = T(data: &memberAssignmentData)
    }
    
    var dataLength: Int {
		return self._errorCode.dataLength + memberAssignment.dataLength + 4
    }
    
    var data: Data {
        return Data()
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: self._errorCode)
    }
	
}


struct HeartbeatRequest: KafkaRequest {
	
	typealias Response = HeartbeatResponse
	var apiKey: ApiKey { return .heartbeatRequest }

    init(
        groupId: String,
        generationId: Int32,
        memberId: String
    ) {
        self.init(
            value: HeartbeatRequestMessage(
                groupId: groupId,
                generationId: generationId,
                memberId: memberId
            )
        )
    }
	
	let value: KafkaType?
    init(value: HeartbeatRequestMessage) {
		self.value = value
    }
}


struct HeartbeatRequestMessage: KafkaType {
    
    private var _groupId: String
    private var _generationId: Int32
    private var _memberId: String

    init(groupId: String, generationId: Int32, memberId: String) {
        _groupId = groupId
        _generationId = generationId
        _memberId = memberId
    }
    
    init(data: inout Data) {
        _groupId = String(data: &data)
        _generationId = Int32(data: &data)
        _memberId = String(data: &data)
    }
    
    var dataLength: Int {
        return self._groupId.dataLength +
            self._generationId.dataLength +
            self._memberId.dataLength
    }
    
	var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._generationId.data)
        data.append(self._memberId.data)
        return data
    }
    
    var groupId: String {
        return _groupId
    }

}


struct HeartbeatResponse: KafkaResponse {
    
    private var _errorCode: Int16
    
    init(data: inout Data) {
        _errorCode = Int16(data: &data)
    }
    
	var dataLength: Int {
        return self._errorCode.dataLength
    }
    
    var data: Data {
        return Data()
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: self._errorCode)
    }
	
}


struct LeaveGroupRequest: KafkaRequest {
	
	typealias Response = LeaveGroupResponse
	
	var apiKey: ApiKey { return .leaveGroupRequest }
    
    init(
        groupId: String,
        memberId: String
    ) {
        self.init(
            value: LeaveGroupRequestMessage(
                groupId: groupId,
                memberId: memberId
            )
        )
    }
	
	let value: KafkaType?
    init(value: LeaveGroupRequestMessage) {
		self.value = value
    }
}


struct LeaveGroupRequestMessage: KafkaType {
    private var _groupId: String
    private var _memberId: String
    
    init(groupId: String, memberId: String) {
        _groupId = groupId
        _memberId = memberId
    }
    
    init(data: inout Data) {
        _groupId = String(data: &data)
        _memberId = String(data: &data)
    }
    
    var dataLength: Int {
        return self._groupId.dataLength +
            self._memberId.dataLength
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._memberId.data)
        return data
    }
    
    var groupId: String {
		return _groupId
    }
}


struct LeaveGroupResponse: KafkaResponse {
    
    private var _errorCode: Int16
    
    init(data: inout Data) {
        _errorCode = Int16(data: &data)
    }
    
    var dataLength: Int {
        return self._errorCode.dataLength
    }
    
    var data: Data {
        return Data()
    }
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: self._errorCode)
    }
}

