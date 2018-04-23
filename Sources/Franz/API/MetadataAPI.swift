//
//  MetadataAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


struct TopicMetadataRequest: KafkaRequest {
	
	typealias Response = MetadataResponse
	static let apiKey: ApiKey = .metadataRequest 
	static let apiVersion: ApiVersion = 0

    init(topic: String) {
        self.init(message: TopicMetadataRequestMessage(values: [topic]))
    }

    init(topics: [String] = []) {
        self.init(message: TopicMetadataRequestMessage(values: topics))
    }
	
	let values: [KafkaType]
    init(message: TopicMetadataRequestMessage) {
		self.values = [message]
    }

}

struct TopicMetadataRequestMessage: KafkaType {

    var values: [String]
    
    init(values: [String]) {
        self.values = values
    }

    init(data: inout Data) {
        values = [String](data: &data)
    }

    var dataLength: Int {
        return self.values.dataLength
    }
    
    var data: Data {
        return (self.values.data)
    }
}

struct MetadataResponse: KafkaResponse {
    
    private var _metadataBrokers: [Broker]
    private var _topicMetadata: [KafkaTopic]
    
    var brokers: [Int32: Broker] {
        var values = [Int32: Broker]()
        for value in _metadataBrokers {
            values[value.nodeId] = value
        }
        return values
    }
    
    var topics: [String: KafkaTopic] {
        var values = [String: KafkaTopic]()
        for value in _topicMetadata {
			values[value.name] = value
        }
        return values
    }
    
    init(data: inout Data) {
        _metadataBrokers = [Broker](data: &data)
        _topicMetadata = [KafkaTopic](data: &data)
    }
	
	var data: Data {
		return _metadataBrokers.data + _topicMetadata.data
	}
	
	var dataLength: Int {
		return _metadataBrokers.dataLength + _topicMetadata.dataLength
	}
}
