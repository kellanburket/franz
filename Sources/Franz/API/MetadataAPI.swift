//
//  MetadataAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


struct TopicMetadataRequest: KafkaRequest {
	
	static let apiKey: ApiKey = .metadataRequest 
	static let apiVersion: ApiVersion = 0

    init(topic: String) {
        self.init(topics: [topic])
    }

    init(topics: [String] = []) {
        self.values = topics
    }
	
	let values: [Encodable]
	
	struct Response: Decodable {
		struct BrokerInfo: Decodable {
			let nodeId: Int32
			let host: String
			let port: Int32
		}
		
		private let _metadataBrokers: [BrokerInfo]
		private let _topicMetadata: [KafkaTopic]
		
		var brokers: [Int32: BrokerInfo] {
			var values = [Int32: BrokerInfo]()
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
	}
}
