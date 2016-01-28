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
        metadata: [String: T],
        memberId: String = "",
        protocolType: GroupProtocol = GroupProtocol.Consumer,
        sessionTimeout: Int32 = 500
    ) {
        self.init(
            value: JoinGroupRequestMessage(
                groupId: groupId,
                sessionTimeout: sessionTimeout,
                memberId: memberId,
                protocolType: protocolType,
                groupProtocols: metadata
            )
        )
    }
    
    init(value: JoinGroupRequestMessage<T>) {
        super.init(apiKey: ApiKey.JoinGroupRequest, value: value)
    }
    
}


class JoinGroupRequestMessage<T: KafkaMetadata>: KafkaClass {
    
    private var _groupId: KafkaString
    private var _sessionTimeout: KafkaInt32
    private var _memberId: KafkaString
    private var _protocolType: KafkaString
    private var _groupProtocols: KafkaProtocol<T>
    
    init(
        groupId: String,
        sessionTimeout: Int32,
        memberId: String = "",
        protocolType: GroupProtocol,
        groupProtocols: [String: T]
    ) {
        _groupId = KafkaString(value: groupId)
        _sessionTimeout = KafkaInt32(value: sessionTimeout)
        _memberId = KafkaString(value: memberId)
        _protocolType = KafkaString(value: protocolType.value)
        _groupProtocols = KafkaProtocol(metadata: groupProtocols)
    }
    
    required init(inout bytes: [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _sessionTimeout = KafkaInt32(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
        _protocolType = KafkaString(bytes: &bytes)
        _groupProtocols = KafkaProtocol(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length +
            self._sessionTimeout.length +
            self._memberId.length +
            self._protocolType.length +
            self._groupProtocols.length
    }()
    
    lazy var data: NSData = {
        var data = NSMutableData(capacity: self.length)!
        data.appendData(self._groupId.data)
        data.appendData(self._sessionTimeout.data)
        data.appendData(self._memberId.data)
        data.appendData(self._protocolType.data)
        data.appendData(self._groupProtocols.data)
        return data
    }()
    
    var groupId: String {
        return _groupId.value
    }
    
    var memberId: String {
        return _memberId.value
    }
    
    var protocolType: String {
        return _protocolType.value
    }
    
    lazy var description: String = {
        return "\tGROUP ID(\(self._groupId.length)): \(self.groupId) => \(self._groupId.data)\n" +
            "\tSESSION TIMEPOUT(\(self._sessionTimeout.length)): \(self._sessionTimeout.value) => \(self._sessionTimeout.data)\n" +
            "\tMEMBER ID(\(self._memberId.length)): \(self.memberId) => \(self._memberId.data)\n" +
            "\tPROTOCOL TYPE(\(self._protocolType.length)): \(self.protocolType) => \(self._protocolType.data)\n" +
            "\tGROUP PROTOCOLS(\(self._groupProtocols.length)):" +
            self._groupProtocols.description
    }()
}

class ConsumerGroupMetadata: KafkaMetadata {
    
    private var _version: KafkaInt16
    private var _subscription: KafkaArray<KafkaString>
    private var _userData: KafkaBytes
    
    init(
        subscription: [String],
        userData: String = "",
        version: ApiVersion = ApiVersion.DefaultVersion
    ) {
        _version = KafkaInt16(value: version.rawValue)
        var values = [KafkaString]()
        for s in subscription {
            values.append(KafkaString(value: s))
        }

        _subscription = KafkaArray(values: values)
        _userData = KafkaBytes(value: userData)
    }
    
    required init(inout bytes: [UInt8]) {
        _version = KafkaInt16(bytes: &bytes)
        _subscription = KafkaArray(bytes: &bytes)
        _userData = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._version.length +
            self._subscription.length +
            self._userData.length
    }()
    
    lazy var data: NSData = {
        var data = NSMutableData(capacity: self.length)!
        data.appendData(self._version.data)
        data.appendData(self._subscription.data)
        data.appendData(self._userData.data)
        return data
    }()
    
    lazy var description: String = {
        return "\t\tVERSION(\(self._version.length)): \(self._version.value) => \(self._version.data)\n" +
            "\t\tSUBSCRIPTION(\(self._subscription.length)):\n" +
            self._subscription.description +
            "\t\tUSER DATA(\(self._userData.length)):\n" +
            self._userData.description
    }()
}


class JoinGroupResponse: KafkaResponse {
    
    private var _errorCode: KafkaInt16
    private var _generationId: KafkaInt32
    private var _groupProtocol: KafkaString
    private var _leaderId: KafkaString
    private var _memberId: KafkaString
    private var _members: KafkaArray<Member>

    required init(inout bytes: [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _generationId = KafkaInt32(bytes: &bytes)
        _groupProtocol = KafkaString(bytes: &bytes)
        _leaderId = KafkaString(bytes: &bytes)
        _memberId = KafkaString(bytes: &bytes)
        _members = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    override var description: String {
        return "JOIN GROUP RESPONSE:\n" +
            "\tERROR CODE: \(_errorCode.data)\n" +
            "\tGENERATION ID: \(_generationId.data)\n" +
            "\tGROUP PROTOCOL: \(_groupProtocol.data)\n" +
            "\tLEADER ID: \(_leaderId.data)\n" +
            "\tMEMBER ID: \(_memberId.data)\n" +
            "\tMEMBERS\n" +
            _members.description
    }
}

class Member: KafkaClass {
    private var _memberName: KafkaString
    private var _memberMetadata: KafkaBytes
    
    var name: String {
        return _memberName.value
    }
    
    var metadata: String {
        return _memberMetadata.value
    }
    
    init(name: String, metadata: String) {
        _memberName = KafkaString(value: name)
        _memberMetadata = KafkaBytes(value: metadata)
    }
    
    required init(inout bytes: [UInt8]) {
        _memberName = KafkaString(bytes: &bytes)
        _memberMetadata = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._memberName.length + self._memberMetadata.length
    }()
    
    lazy var data: NSData = {
        var data = NSMutableData(capacity: self.length)!
        data.appendData(self._memberName.data)
        data.appendData(self._memberMetadata.data)
        return data
    }()
    
    lazy var description: String = {
        return "\t\tNAME(\(self._memberName.length)): \(self.name) => \(self._memberName.data)\n" +
            "\t\tMETADATA(\(self._memberMetadata.length)): \(self.metadata) => \(self._memberMetadata.data)"
    }()
}


/*
// SyncGroupRequest

class SyncGroupRequestMessage<T: KafkaMetadata>: KafkaClass {

}

// GroupAssignment
// SuncGroupResponse
// HeartbeatRequest
// HeartbeatRequestMessage
// HeartbeatResponse
// LeaveGroupRequest
// LeaveGroupRequestMessage
// LeaveGroupResponse
*/


