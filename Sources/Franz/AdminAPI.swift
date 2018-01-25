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
        super.init(apiKey: ApiKey.listGroupsRequest)
    }
}


class ListGroupsResponse: KafkaResponse {
    
    private var _errorCode: Int16
    private var _groups: [ListedGroup]
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var groups: [String: String] {
        var groups = [String: String]()
        
        for group in _groups {
            groups[group.id] = group.groupProtocolType
        }
        
        return groups
    }
    
	required init(data: inout Data) {
        _errorCode = Int16(data: &data)
        _groups = [ListedGroup](data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength +
            self._groups.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
		data.append(self._errorCode.data)
		data.append(self._groups.data)
        return data
    }()
}


class ListedGroup: KafkaType {
    private var _groupId: String
    private var _protocolType: String
    
    var id: String {
        return _groupId
    }
    
    var groupProtocolType: String {
        return _protocolType
    }
    
	required init(data: inout Data) {
        _groupId = String(data: &data)
        _protocolType = String(data: &data)
    }
    
    
    lazy var dataLength: Int = {
        return  self._groupId.dataLength +
            self._protocolType.dataLength
        
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
		data.append(self._groupId.data)
		data.append(self._protocolType.data)
        return data
    }()
}


class DescribeGroupsRequest: KafkaRequest {

    convenience init(id: String) {
        self.init(value: DescribeGroupsRequestMessage(groupIds: [id]))
    }

    convenience init(ids: [String]) {
        self.init(value: DescribeGroupsRequestMessage(groupIds: ids))
    }

    init(value: DescribeGroupsRequestMessage) {
        super.init(apiKey: ApiKey.describeGroupsRequest, value: value)
    }
}


class DescribeGroupsRequestMessage: KafkaType {
    private var _groupIds: [String]
    
    init(groupIds: [String]) {
        var values = [String]()
        for value in groupIds {
            values.append(value)
        }
        _groupIds = groupIds
    }
    
	required init(data: inout Data) {
        _groupIds = [String](data: &data)
    }
    
    lazy var dataLength: Int = {
        return  self._groupIds.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
		data.append(self._groupIds.data)
        return data
    }()
}


class DescribeGroupsResponse: KafkaResponse {
	
    private(set) var states: [GroupStateResponse]
    
    required init(data: inout Data) {
        states = [GroupStateResponse](data: &data)
    }
    
    lazy var dataLength: Int = {
        return states.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: dataLength)
        data.append(states.data)
        return data
    }()
}


class GroupStateResponse: KafkaType {
    private var _errorCode: Int16
    private var _groupId: String
    private var _state: String
    private var _protocolType: String
    private var _protocol: String
    
    var id: String? {
        return _groupId
    }
    
    var kafkaProtocol: String? {
        return _protocol
    }
    
    var protocolType: GroupProtocol {
		if _protocolType == "consumer" {
			return GroupProtocol.consumer
		} else {
			return GroupProtocol.custom(name: _protocolType)
		}
    }

    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var state: GroupState {
		return GroupState(rawValue: _state)!
    }
    
    private(set) var members: [GroupMemberResponse]

    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
        _groupId = String(data: &data)
        _state = String(data: &data)
        _protocolType = String(data: &data)
        _protocol = String(data: &data)
        members = [GroupMemberResponse](data: &data)
    }
    
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength +
            self._groupId.dataLength +
            self._state.dataLength +
            self._protocolType.dataLength +
            self._protocol.dataLength +
            self.members.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
		data.append(self._groupId.data)
		data.append(self._protocolType.data)
		//TODO: this looks broken?
        return data
    }()
	
}


class GroupMemberResponse: KafkaType {
    private var _memberId: String
    private var _clientId: String
    private var _clientHost: String
    private var _memberMetadata: Data
    private var _memberAssignment: Data
    
    var memberId: String {
        return _memberId
    }

    var clientId: String {
        return _clientId
    }
    
    var host: String {
        return _clientHost
    }
    
    required init(data: inout Data) {
        _memberId = String(data: &data)
        _clientId = String(data: &data)
        _clientHost = String(data: &data)
        _memberMetadata = Data(data: &data)
        _memberAssignment = Data(data: &data)
    }
    
    
    lazy var dataLength: Int = {
        return self._memberId.dataLength +
            self._clientId.dataLength +
            self._clientHost.dataLength +
            self._memberMetadata.dataLength +
            self._memberAssignment.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        return data
    }()
}
