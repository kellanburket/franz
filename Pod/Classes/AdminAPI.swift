//
//  AdminAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class ListGroupsRequest: KafkaRequest {
    init() {
        super.init(apiKey: ApiKey.ListGroupsRequest)
    }
}


class DescribeGroupsRequest: KafkaRequest {

    convenience init(id: String) {
        self.init(value: DescribeGroupsRequestMessage(groupId: id))
    }
    
    init(value: DescribeGroupsRequestMessage) {
        super.init(apiKey: ApiKey.DescribeGroupsRequest, value: value)
    }
}


class DescribeGroupsRequestMessage: KafkaClass {
    private var _groupId: KafkaString
    
    var id: String {
        return _groupId.value
    }

    init(groupId: String) {
        _groupId = KafkaString(value: groupId)
    }
    
    required init(inout bytes: [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return  self._groupId.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._groupId.data)
        return data
    }()
    
    lazy var description: String = {
        return "GROUP ID(\(self._groupId.length)): \(self.id) => \(self._groupId.data)"
    }()
}


class DescribeGroupsResponse: KafkaResponse {
    private var _groups: KafkaArray<GroupResponse>
    
    required init(inout bytes: [UInt8]) {
        _groups = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groups.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._groups.data)
        return data
    }()
    
    override var description: String {
        return "DESCRIBE GROUP RESPONSE:\n" +
            "\tGROUPS:\n" + self._groups.description
    }
}


class GroupResponse: KafkaClass {
    private var _errorCode: KafkaInt16
    private var _groupId: KafkaString
    private var _state: KafkaString
    private var _protocolType: KafkaString
    private var _protocol: KafkaString
    private var _members: KafkaArray<GroupMemberResponse>
    
    var id: String {
        return _groupId.value
    }
    
    var type: String {
        return _protocolType.value
    }
    
    var state: String {
        return _state.value
    }
    
    var kafkaProtocol: String {
        return _protocol.value
    }

    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var members: [GroupMemberResponse] {
        return _members.values
    }

    required init(inout bytes: [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _groupId = KafkaString(bytes: &bytes)
        _state = KafkaString(bytes: &bytes)
        _protocolType = KafkaString(bytes: &bytes)
        _protocol = KafkaString(bytes: &bytes)
        _members = KafkaArray(bytes: &bytes)
    }
    
    
    lazy var length: Int = {
        return self._errorCode.length +
            self._groupId.length +
            self._state.length +
            self._protocolType.length +
            self._protocol.length +
            self._members.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._groupId.data)
        data.appendData(self._protocolType.data)
        return data
    }()
    
    lazy var description: String = {
        return "\tERROR CODE: \(self.error?.code ?? 0)\n" +
            "\tERROR DESCRIPTION: \(self.error?.description ?? String())\n" +
            "\tGROUP ID(\(self._groupId.length)): \(self.id) => \(self._groupId.data)\n" +
            "\tSTATE(\(self._state.length)): \(self.state) => \(self._state.data)" +
            "\tPROTOCOL TYPE(\(self._protocolType.length)): \(self.type) => \(self._protocolType.data)\n" +
            "\tPROTOCOL(\(self._protocol.length)): \(self.kafkaProtocol) => \(self._protocol.data)" +
            "\tMEMBERS(\(self._members.length)):\n" + self._members.description
    }()
}


class GroupMemberResponse: KafkaClass {
    private var _memberId: KafkaString
    private var _clientId: KafkaString
    private var _clientHost: KafkaString
    private var _memberMetadata: KafkaBytes
    private var _memberAssignment: KafkaBytes
    
    var memberId: String {
        return _memberId.value
    }

    var clientId: String {
        return _clientId.value
    }
    
    var host: String {
        return _clientHost.value
    }
    
    required init(inout bytes: [UInt8]) {
        _memberId = KafkaString(bytes: &bytes)
        _clientId = KafkaString(bytes: &bytes)
        _clientHost = KafkaString(bytes: &bytes)
        _memberMetadata = KafkaBytes(bytes: &bytes)
        _memberAssignment = KafkaBytes(bytes: &bytes)
    }
    
    
    lazy var length: Int = {
        return self._memberId.length +
            self._clientId.length +
            self._clientHost.length +
            self._memberMetadata.length +
            self._memberAssignment.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        return data
    }()
    
    lazy var description: String = {
        return "\t\tMEMBER ID: \(self.memberId)\n" +
            "\t\tCLIENT ID: \(self.clientId)\n" +
            "\t\tCLIENT HOST: \(self.host)\n" +
            "\t\tMEMBER METADATA: \(self._memberMetadata.value)\n" +
            "\t\tMEMBER METADATA: \(self._memberAssignment.value)\n"
    }()
}


class ListGroupsResponse: KafkaResponse {
    
    private var _errorCode: KafkaInt16
    private var _groups: KafkaArray<ListedGroup>
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var groups: [String: String] {
        var groups = [String: String]()
        for group in _groups.values {
            groups[group.id] = group.type
        }

        return groups
    }
    
    required init(inout bytes: [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _groups = KafkaArray(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length +
            self._groups.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._errorCode.data)
        data.appendData(self._groups.data)
        return data
    }()
    
    override var description: String {
        return "LIST GROUPS RESPONSE:\n" +
            "\tERROR CODE: \(self.error?.code ?? 0)\n" +
            "\tERROR DESCRIPTION: \(self.error?.description ?? String())\n" +
            "\tGROUPS(\(self._groups.length)):\n" +
            _groups.description
    }
}


class ListedGroup: KafkaClass {
    private var _groupId: KafkaString
    private var _protocolType: KafkaString

    var id: String {
        return _groupId.value
    }
    
    var type: String {
        return _protocolType.value
    }

    required init(inout bytes: [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
        _protocolType = KafkaString(bytes: &bytes)
    }

    
    lazy var length: Int = {
        return  self._groupId.length +
            self._protocolType.length

    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._groupId.data)
        data.appendData(self._protocolType.data)
        return data
    }()
    
    lazy var description: String = {
        return "\t\tGROUP ID(\(self._groupId.length)): \(self.id) => \(self._groupId.data)\n" +
            "\t\tPROTOCOL TYPE(\(self._protocolType.length)): \(self.type) => \(self._protocolType.data)\n"
    }()
}
