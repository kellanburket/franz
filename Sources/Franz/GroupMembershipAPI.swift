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


class JoinGroupRequestMessage<T: KafkaMetadata>: KafkaClass {
    
    fileprivate var _groupId: KafkaString
    fileprivate var _sessionTimeout: KafkaInt32
    fileprivate var _memberId: KafkaString
    fileprivate var _protocolType: KafkaString
    fileprivate var _groupProtocols: KafkaArray<JoinGroupProtocol<T>>
    
    init(
        groupId: String,
        sessionTimeout: Int32,
        memberId: String,
        protocolType: GroupProtocol,
        groupProtocols: [AssignmentStrategy: T]
    ) {
        _groupId = KafkaString(value: groupId)
        _sessionTimeout = KafkaInt32(value: sessionTimeout)
        _memberId = KafkaString(value: memberId)
        _protocolType = KafkaString(value: protocolType.value)
        var values = [JoinGroupProtocol<T>]()

        for (name, metadata) in groupProtocols {
            values.append(JoinGroupProtocol(name: name.rawValue, metadata: metadata))
        }
            
        _groupProtocols = KafkaArray(values: values)
    }
    
	required init( bytes: inout [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _sessionTimeout = KafkaInt32(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
        _protocolType = KafkaString(bytes: &bytes)
        _groupProtocols = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length +
            self._sessionTimeout.length +
            self._memberId.length +
            self._protocolType.length +
            self._groupProtocols.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._groupId.data as Data)
        data.append(self._sessionTimeout.data as Data)
        data.append(self._memberId.data as Data)
        data.append(self._protocolType.data as Data)
        data.append(self._groupProtocols.data as Data)
        return data as Data
    }()
    
    var groupId: String {
        return _groupId.value ?? String()
    }
    
    var protocolType: String {
        return _protocolType.value ?? String()
    }
    
    lazy var description: String = {
        return "\tGROUP ID(\(self._groupId.length)): " +
                "\(self.groupId) => \(self._groupId.data)\n" +
            "\tSESSION TIMEPOUT(\(self._sessionTimeout.length)): " +
                "\(self._sessionTimeout.value) => \(self._sessionTimeout.data)\n" +
            "\tMEMBER ID(\(self._memberId.length)): " +
                "\(self._memberId.value) => \(self._memberId.data)\n" +
            "\tPROTOCOL TYPE(\(self._protocolType.length)): \(self.protocolType) => \(self._protocolType.data)\n" +
            "\tGROUP PROTOCOLS(\(self._groupProtocols.length)):\n" +
            self._groupProtocols.description
    }()
}

class JoinGroupProtocol<T: KafkaMetadata>: KafkaClass {
    fileprivate var _protocolName: KafkaString
    fileprivate var _protocolMetadata: T

    init(name: String, metadata: T) {
        _protocolName = KafkaString(value: name)
        _protocolMetadata = metadata
    }
    
	required init( bytes: inout [UInt8]) {
        _protocolName = KafkaString(bytes: &bytes)
        _protocolMetadata = T(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._protocolName.length +
            self._protocolMetadata.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._protocolName.data as Data)
        data.append(self._protocolMetadata.data as Data)
        return data as Data
    }()
    
    lazy var description: String = {
        return "\t\tPROTOCOL NAME(\(self._protocolName.length)): " +
            "\(self._protocolName.value) => \(self._protocolName.data)\n" +
            "\t\tPROTOCOL METADATA(\(self._protocolMetadata.length)):\n" +
            self._protocolMetadata.description
    }()

}

class ConsumerGroupMetadata: KafkaMetadata {
    
    fileprivate var _version: KafkaInt16
    fileprivate var _subscription: KafkaArray<KafkaString>
    fileprivate var _userData: KafkaBytes
    
    static var protocolType: GroupProtocol {
        return GroupProtocol.consumer
    }

    init(
        subscription: [String],
        userData: Data? = nil,
        version: ApiVersion = ApiVersion.defaultVersion
    ) {
        _version = KafkaInt16(value: version.rawValue)
        var values = [KafkaString]()
        for s in subscription {
            values.append(KafkaString(value: s))
        }

        _subscription = KafkaArray(values: values)
		_userData = KafkaBytes(value: userData)
    }
    
    required init(bytes: inout [UInt8]) {
        _version = KafkaInt16(bytes: &bytes)
        _subscription = KafkaArray(bytes: &bytes)
        _userData = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self.sizeDataLength + self.valueDataLength
    }()
    
    lazy var sizeDataLength: Int = {
        return 4
    }()
    
    lazy var valueDataLength: Int = {
        return self._version.length +
            self._subscription.length +
            self._userData.length
    }()
    
    fileprivate lazy var sizeData: Data = {
        return (Int32(self.valueDataLength).data as Data)
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self.sizeData)
        data.append(self._version.data as Data)
        data.append(self._subscription.data as Data)
        data.append(self._userData.data as Data)
        return data as Data
    }()
    
    lazy var description: String = {
        return "\t\t\tSIZE(4): \(self.sizeData) => \(self.length)\n" +
            "\t\t\tVERSION(\(self._version.length)): \(self._version.value) => \(self._version.data)\n" +
            "\t\t\tSUBSCRIPTION(\(self._subscription.length)):\n" +
            "\t\t\t\t\(self._subscription.description)\n" +
            "\t\t\tUSER DATA(\(self._userData.length)):\n" +
            "\t\t\t\t\(self._userData.description)"
    }()
}


class JoinGroupResponse: KafkaResponse {
    
    fileprivate var _errorCode: KafkaInt16
    fileprivate var _generationId: KafkaInt32
    fileprivate var _groupProtocol: KafkaString
    fileprivate var _leaderId: KafkaString
    fileprivate var _memberId: KafkaString
    fileprivate var _members: KafkaArray<Member>
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var generationId: Int32 {
        return _generationId.value
    }
    
    var memberId: String {
        return _memberId.value ?? String()
    }
    
    var leaderId: String {
        return _leaderId.value ?? String()
    }
    
    var groupProtocol: GroupProtocol {
        if _groupProtocol.value == GroupProtocol.consumer.value {
            return GroupProtocol.consumer
        } else {
            return GroupProtocol.custom(name: _groupProtocol.value ?? String())
        }
    }
    
    var members: [Member] {
        return _members.values
    }
    
    required init(bytes: inout [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _generationId = KafkaInt32(bytes: &bytes)
        _groupProtocol = KafkaString(bytes: &bytes)
        _leaderId = KafkaString(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
        _members = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    override var description: String {
        let error = self.error?.description ?? String()
        
        return "JOIN GROUP RESPONSE:\n" +
            "\tERROR CODE(\(_errorCode.length)): \(error) => \(_errorCode.data)\n" +
            "\tGENERATION ID(\(_generationId.length)): \(generationId) => \(_generationId.data)\n" +
            "\tGROUP PROTOCOL(\(_groupProtocol.length)): \(groupProtocol) => \(_groupProtocol.data)\n" +
            "\tLEADER ID(\(_groupProtocol.length)): \(leaderId) => \(_leaderId.data)\n" +
            "\tMEMBER ID(\(_memberId.length)): \(memberId) => \(_memberId.data)\n" +
            "\tMEMBERS(\(_members.length)):\n" +
            _members.description
    }
}

class Member: KafkaClass {
    fileprivate var _memberName: KafkaString
    fileprivate var _memberMetadata: KafkaBytes
    
    var name: String {
        return _memberName.value ?? String()
    }
    
    init(name: String, metadata: String) {
        _memberName = KafkaString(value: name)
        _memberMetadata = KafkaBytes(value: metadata)
    }
    
    required init(bytes: inout [UInt8]) {
        _memberName = KafkaString(bytes: &bytes)
        _memberMetadata = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._memberName.length + self._memberMetadata.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._memberName.data as Data)
        data.append(self._memberMetadata.data as Data)
        return data as Data
    }()
    
    lazy var description: String = {
        return "\t\tNAME(\(self._memberName.length)): \(self.name) => \(self._memberName.data)\n" +
            "\t\tMETADATA(\(self._memberMetadata.length)): \(self._memberMetadata) => \(self._memberMetadata.data)"
    }()
}


class SyncGroupRequest<T: KafkaMetadata>: KafkaRequest {
    
    convenience init(
        groupId: String,
        generationId: Int32,
        memberId: String,
        groupAssignment: [T]
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
        super.init(apiKey: ApiKey.syncGroupRequest, value: value)
    }
}

class GroupMemberAssignment: KafkaMetadata {
    fileprivate var _version: KafkaInt16
    fileprivate var _partitionAssignment: KafkaArray<PartitionAssignment>
    fileprivate var _userData: KafkaBytes

    static var protocolType: GroupProtocol {
        return GroupProtocol.consumer
    }

    init(topics: [String: [Int32]], userData: Data, version: ApiVersion) {
        _version = KafkaInt16(value: version.rawValue)

        var values = [PartitionAssignment]()
        for (topic, partitions) in topics {
            values.append(
                PartitionAssignment(topic: topic, partitions: partitions)
            )
        }
        _partitionAssignment = KafkaArray(values: values)
		_userData = KafkaBytes(value: userData)
    }

    required init(bytes: inout [UInt8]) {
        _version = KafkaInt16(bytes: &bytes)
        _partitionAssignment = KafkaArray(bytes: &bytes)
        _userData = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._version.length +
            self._partitionAssignment.length +
            self._userData.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._version.data as Data)
        data.append(self._partitionAssignment.data as Data)
        data.append(self._userData.data as Data)
        return data as Data
    }()
    
    lazy var description: String = {
        return "\t\tVERSION(\(self._version.length)): \(self._version.data) => \(self._version.value)\n" +
            "\t\tPARTITION ASSIGNMENT(\(self._partitionAssignment.length)):\n" +
            self._partitionAssignment.description +
            "\t\tUSER DATA\(self._userData.length): \(self._userData.data) => \(self._userData.value)\n"
    }()
}

class PartitionAssignment: KafkaClass {
    fileprivate var _topic: KafkaString
    fileprivate var _partitions: KafkaArray<KafkaInt32>
    
    init(topic: String, partitions: [Int32]) {
        _topic = KafkaString(value: topic)
        var values = [KafkaInt32]()
        for partition in partitions {
            values.append(KafkaInt32(value: partition))
        }
        _partitions = KafkaArray(values: values)
    }

    required init(bytes: inout [UInt8]) {
        _topic = KafkaString(bytes: &bytes)
        _partitions = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._topic.length + self._partitions.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._topic.data as Data)
        data.append(self._partitions.data as Data)
        return data as Data
    }()
    
    lazy var description: String = {
        return "\t\t\tTOPIC(\(self._topic.length)): \(self._topic.value)\n" +
            "\t\t\tPARTITIONS(\(self._partitions.length)): \(self._partitions.values)"
    }()
}

class SyncGroupRequestMessage<T: KafkaMetadata>: KafkaClass {
    
    fileprivate var _groupId: KafkaString
    fileprivate var _generationId: KafkaInt32
    fileprivate var _memberId: KafkaString
    fileprivate var _groupAssignment: KafkaArray<T>

    init(
        groupId: String,
        generationId: Int32,
        memberId: String,
        groupAssignment: [T]
    ) {
        _groupId = KafkaString(value: groupId)
        _generationId = KafkaInt32(value: generationId)
        _memberId = KafkaString(value: memberId)
        _groupAssignment = KafkaArray(values: groupAssignment)
    }
    
    required init(bytes: inout [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _generationId = KafkaInt32(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
        _groupAssignment = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length +
            self._generationId.length +
            self._memberId.length +
            self._groupAssignment.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._groupId.data as Data)
        data.append(self._generationId.data as Data)
        data.append(self._memberId.data as Data)
        data.append(self._groupAssignment.data as Data)
        return data as Data
    }()
    
    var groupId: String {
        return _groupId.value ?? String()
    }
    
    lazy var description: String = {
        return "\tGROUP ID(\(self._groupId.length)): "
    }()
}


class SyncGroupResponse<T: KafkaMetadata>: KafkaResponse {
    
    fileprivate var _errorCode: KafkaInt16
    fileprivate var _memberAssignment: T
    
    required init(bytes: inout [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _memberAssignment = T(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length + self._memberAssignment.length
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode.value)
    }()
    
    override var description: String {
        return "SYNC GROUP RESPONSE(\(self.length))\n" +
            "\tERROR(\(self._errorCode.length)): \(self.error?.description ?? String())\n" +
            "\tMEMBER ASSIGNMENT(\(self._memberAssignment.length)):\n" +
            self._memberAssignment.description
    }
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


class HeartbeatRequestMessage: KafkaClass {
    
    fileprivate var _groupId: KafkaString
    fileprivate var _generationId: KafkaInt32
    fileprivate var _memberId: KafkaString

    init(groupId: String, generationId: Int32, memberId: String) {
        _groupId = KafkaString(value: groupId)
        _generationId = KafkaInt32(value: generationId)
        _memberId = KafkaString(value: memberId)
    }
    
    required init(bytes: inout [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _generationId = KafkaInt32(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length +
            self._generationId.length +
            self._memberId.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._groupId.data as Data)
        data.append(self._generationId.data as Data)
        data.append(self._memberId.data as Data)
        return data as Data
    }()
    
    var groupId: String {
        return _groupId.value ?? String()
    }
    
    lazy var description: String = {
        return "\tGROUP ID(\(self._groupId.length)): "
    }()
}


class HeartbeatResponse: KafkaResponse {
    
    fileprivate var _errorCode: KafkaInt16
    
    required init(bytes: inout [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode.value)
    }()
    
    override var description: String {
        return "\tERROR(\(self._errorCode.length)): \(self.error?.description ?? String())"
    }
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


class LeaveGroupRequestMessage: KafkaClass {
    fileprivate var _groupId: KafkaString
    fileprivate var _memberId: KafkaString
    
    init(groupId: String, memberId: String) {
        _groupId = KafkaString(value: groupId)
        _memberId = KafkaString(value: memberId)
    }
    
    required init(bytes: inout [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length +
            self._memberId.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._groupId.data as Data)
        data.append(self._memberId.data as Data)
        return data as Data
    }()
    
    var groupId: String {
        return _groupId.value ?? String()
    }
    
    lazy var description: String = {
        return "\tGROUP ID(\(self._groupId.length)): "
    }()
}


class LeaveGroupResponse: KafkaResponse {
    
    fileprivate var _errorCode: KafkaInt16
    
    required init(bytes: inout [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length
    }()
    
    lazy var data: Data = {
        return Data()
    }()
    
    lazy var error: KafkaErrorCode? = {
        return KafkaErrorCode(rawValue: self._errorCode.value)
    }()
    
    override var description: String {
        return "\tERROR(\(self._errorCode.length)): \(self.error?.description ?? String())"
    }
}
