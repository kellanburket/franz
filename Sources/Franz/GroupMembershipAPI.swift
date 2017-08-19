//
//  GroupMembershipAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


class GroupMembershipRequest<T: KafkaMetadata>: KafkaRequest {

    convenience init(
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
    
    init(value: JoinGroupRequestMessage<T>) {
        super.init(apiKey: ApiKey.joinGroupRequest, value: value)
    }
}


class JoinGroupRequestMessage<T: KafkaMetadata>: KafkaType {
    
    private var _groupId: String
    private var _sessionTimeout: Int32
    private var _memberId: String
    private var _protocolType: String
    private var _groupProtocols: KafkaArray<JoinGroupProtocol<T>>
    
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
            
        _groupProtocols = KafkaArray(values)
    }
    
	required init(data: inout Data) {
        _groupId = String(data: &data)
        _sessionTimeout = Int32(data: &data)
        _memberId = String(data: &data)
        _protocolType = String(data: &data)
        _groupProtocols = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._groupId.dataLength +
            self._sessionTimeout.dataLength +
            self._memberId.dataLength +
            self._protocolType.dataLength +
            self._groupProtocols.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._sessionTimeout.data)
        data.append(self._memberId.data)
        data.append(self._protocolType.data)
        data.append(self._groupProtocols.data)
        return data
    }()
    
    var groupId: String {
		return _groupId
    }
    
    var protocolType: String {
		return _protocolType
    }
}

class JoinGroupProtocol<T: KafkaMetadata>: KafkaType {
    private var _protocolName: String
    private var _protocolMetadata: T

    init(name: String, metadata: T) {
        _protocolName = name
        _protocolMetadata = metadata
    }
    
	required init(data: inout Data) {
        _protocolName = String(data: &data)
        _protocolMetadata = T(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._protocolName.dataLength +
            self._protocolMetadata.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._protocolName.data)
        data.append(self._protocolMetadata.data)
        return data
    }()

}

class ConsumerGroupMetadata: KafkaMetadata {
    
    private var _version: Int16
    private var _subscription: KafkaArray<String>
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
        var values = [String]()
        for s in subscription {
            values.append(s)
        }

        _subscription = KafkaArray(values)
		_userData = userData
    }
    
    required init(data: inout Data) {
        _version = Int16(data: &data)
        _subscription = KafkaArray(data: &data)
        _userData = Data(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self.sizeDataLength + self.valueDataLength
    }()
    
    lazy var sizeDataLength: Int = {
        return 4
    }()
    
    lazy var valueDataLength: Int = {
        return self._version.dataLength +
            self._subscription.dataLength +
            self._userData.dataLength
    }()
    
    private lazy var sizeData: Data = {
        return (Int32(self.valueDataLength).data)
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self.sizeData)
        data.append(self._version.data)
        data.append(self._subscription.data)
        data.append(self._userData.data)
        return data
    }()
}


class JoinGroupResponse: KafkaResponse {
    
    private var _errorCode: Int16
    private var _generationId: Int32
    private var _groupProtocol: String
    private var _leaderId: String
    private var _memberId: String
    private var _members: KafkaArray<Member>
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var generationId: Int32 {
        return _generationId
    }
    
    var memberId: String {
		return _memberId
    }
    
    var leaderId: String {
		return _leaderId
    }
    
    var groupProtocol: GroupProtocol {
        if _groupProtocol == GroupProtocol.consumer.value {
            return GroupProtocol.consumer
        } else {
			return GroupProtocol.custom(name: _groupProtocol)
        }
    }
    
    var members: [Member] {
        return _members.values
    }
    
    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
        _generationId = Int32(data: &data)
        _groupProtocol = String(data: &data)
        _leaderId = String(data: &data)
        _memberId = String(data: &data)
        _members = KafkaArray(data: &data)
    }
	
	var data: Data {
		let values: [KafkaType] = [_errorCode, _generationId, _groupProtocol, _leaderId, _memberId, _members]
		return values.map { $0.data }.reduce(Data(), +)
	}
	
	var dataLength: Int {
		let values: [KafkaType] = [_errorCode, _generationId, _groupProtocol, _leaderId, _memberId, _members]
		return values.map { $0.dataLength }.reduce(0, +)
	}
}

class Member: KafkaType {
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
    
    required init(data: inout Data) {
        _memberName = String(data: &data)
        _memberMetadata = Data(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._memberName.dataLength + self._memberMetadata.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._memberName.data)
        data.append(self._memberMetadata.data)
        return data
    }()
}


class SyncGroupRequest<T: KafkaMetadata>: KafkaRequest {
    
    convenience init(
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
    
    init(value: SyncGroupRequestMessage<T>) {
        super.init(apiKey: .syncGroupRequest, value: value)
    }
}

class GroupMemberAssignment: KafkaMetadata {
    private var _version: Int16
    let partitionAssignment: KafkaArray<PartitionAssignment>
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
        partitionAssignment = KafkaArray(values)
		_userData = userData
    }

    required init(data: inout Data) {
        _version = Int16(data: &data)
        partitionAssignment = KafkaArray(data: &data)
        _userData = Data(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._version.dataLength +
            self.partitionAssignment.dataLength +
            self._userData.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._version.data)
        data.append(self.partitionAssignment.data)
        data.append(self._userData.data)
        return data
    }()
}

class PartitionAssignment: KafkaType {
    let topic: String
    let partitions: KafkaArray<Int32>
    
    init(topic: String, partitions: [Int32]) {
        self.topic = topic
        var values = [Int32]()
        for partition in partitions {
            values.append(partition)
        }
        self.partitions = KafkaArray(values)
    }

    required init(data: inout Data) {
        topic = String(data: &data)
        partitions = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self.topic.dataLength + self.partitions.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self.topic.data)
        data.append(self.partitions.data)
        return data
    }()
}

typealias MemberId = String

class SyncGroupRequestMessage<T: KafkaMetadata>: KafkaType {
    
    private var _groupId: String
    private var _generationId: Int32
    private var _memberId: String
    private var _groupAssignment: KafkaArray<GroupAssignment<T>>

    init(
        groupId: String,
        generationId: Int32,
        memberId: String,
        groupAssignment: [String: T]
    ) {
        _groupId = groupId
        _generationId = generationId
        _memberId = memberId
		_groupAssignment = KafkaArray(groupAssignment.map { GroupAssignment<T>(memberId: $0, memberAssignment: $1) })
    }
    
    required init(data: inout Data) {
        _groupId = String(data: &data)
        _generationId = Int32(data: &data)
        _memberId = String(data: &data)
        _groupAssignment = KafkaArray(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._groupId.dataLength +
            self._generationId.dataLength +
            self._memberId.dataLength +
            self._groupAssignment.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._generationId.data)
        data.append(self._memberId.data)
        data.append(self._groupAssignment.data)
        return data
    }()
    
    var groupId: String {
        return _groupId
    }
	
	
	
	class GroupAssignment<T: KafkaMetadata>: KafkaType {
		
		let memberId: String
		let memberAssignment: T
		
		private var memberAssignmentData: Data {
			return Data(memberAssignment.data)
		}
		
		required init(data: inout Data) {
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


class SyncGroupResponse<T: KafkaMetadata>: KafkaResponse {
    
    private var _errorCode: Int16
    let memberAssignment: T
    
    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
		var memberAssignmentData = Data(data: &data)
		memberAssignment = T(data: &memberAssignmentData)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength + self.memberAssignment.dataLength
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode)
    }()
	
}


class HeartbeatRequest: KafkaRequest {

    convenience init(
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
    
    init(value: HeartbeatRequestMessage) {
        super.init(apiKey: ApiKey.heartbeatRequest, value: value)
    }
}


class HeartbeatRequestMessage: KafkaType {
    
    private var _groupId: String
    private var _generationId: Int32
    private var _memberId: String

    init(groupId: String, generationId: Int32, memberId: String) {
        _groupId = groupId
        _generationId = generationId
        _memberId = memberId
    }
    
    required init(data: inout Data) {
        _groupId = String(data: &data)
        _generationId = Int32(data: &data)
        _memberId = String(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._groupId.dataLength +
            self._generationId.dataLength +
            self._memberId.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._generationId.data)
        data.append(self._memberId.data)
        return data
    }()
    
    var groupId: String {
        return _groupId
    }

}


class HeartbeatResponse: KafkaResponse {
    
    private var _errorCode: Int16
    
    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode)
    }()
	
}


class LeaveGroupRequest: KafkaRequest {
    
    convenience init(
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
    
    init(value: LeaveGroupRequestMessage) {
        super.init(apiKey: ApiKey.leaveGroupRequest, value: value)
    }
}


class LeaveGroupRequestMessage: KafkaType {
    private var _groupId: String
    private var _memberId: String
    
    init(groupId: String, memberId: String) {
        _groupId = groupId
        _memberId = memberId
    }
    
    required init(data: inout Data) {
        _groupId = String(data: &data)
        _memberId = String(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._groupId.dataLength +
            self._memberId.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        data.append(self._memberId.data)
        return data
    }()
    
    var groupId: String {
		return _groupId
    }
}


class LeaveGroupResponse: KafkaResponse {
    
    private var _errorCode: Int16
    
    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode)
    }()
}

