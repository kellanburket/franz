//
//  AdminAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/19/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

struct ListGroupsRequest: KafkaRequest {
	static let apiVersion: ApiVersion = 0
	
	static let apiKey: ApiKey = .listGroupsRequest 
	
	let values: [Encodable] = []
	typealias Response = ListGroupsResponse
}


struct ListGroupsResponse: Decodable {
    
    private var errorCode: Int16
    private var listedGroups: [ListedGroup]
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: errorCode)
    }
    
    var groups: [String: String] {
        var groups = [String: String]()
        
        for group in listedGroups {
            groups[group.id] = group.protocolType
        }
        
        return groups
    }
	
}


struct ListedGroup: Codable {
    let id: String
    let protocolType: String
}


struct DescribeGroupsRequest: KafkaRequest {
	static let apiVersion: ApiVersion = 0
	
	typealias Response = DescribeGroupsResponse
	
	let values: [Encodable]
	
	static let apiKey: ApiKey = .describeGroupsRequest 
	
    init(id: String) {
		self.init(value: DescribeGroupsRequestMessage(groupIds: [id]))
    }

    init(ids: [String]) {
		self.init(value: DescribeGroupsRequestMessage(groupIds: ids))
    }

    init(value: DescribeGroupsRequestMessage) {
		self.values = [value]
    }
}


struct DescribeGroupsRequestMessage: Encodable {
    let groupIds: [String]
}

struct DescribeGroupsResponse: Decodable {
    let states: [GroupStateResponse]
}


struct GroupStateResponse: Decodable {
    let error: KafkaErrorCode
    let id: String
    let state: GroupState
    private let _protocolType: String
    let kafkaProtocol: String
	let members: [GroupMemberResponse]
    
    var protocolType: GroupProtocol {
		if _protocolType == "consumer" {
			return GroupProtocol.consumer
		} else {
			return GroupProtocol.custom(name: _protocolType)
		}
    }
	
}


struct GroupMemberResponse: Decodable {
    let memberId: String
    let clientId: String
    let clientHost: String
    let memberMetadata: Data
    let memberAssignment: Data
}
