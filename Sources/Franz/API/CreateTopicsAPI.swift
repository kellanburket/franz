//
//  CreateTopicsAPI.swift
//  Franz
//
//  Created by Luke Lau on 22/04/2018.
//

import Foundation

struct CreateTopicsRequest: KafkaRequest {
	
	static let apiKey: ApiKey = .createTopics
	static let apiVersion: ApiVersion = 2
	
	var values: [Encodable] { return [requests, timeout, validateOnly] }
	
	let requests: [CreateTopicRequest]
	let timeout: Int32
	let validateOnly: Bool
	
	init(requests: [CreateTopicRequest], timeout: Int32 = 0, validateOnly: Bool = false) {
		self.requests = requests
		self.timeout = timeout
		self.validateOnly = validateOnly
	}
	
	struct CreateTopicRequest: Encodable {
		let topic: String
		let numPartitions: Int32
		let replicationFactor: Int16
		let replicaAssignments: [ReplicaAssignment]
		let configEntries: [ConfigEntry]
		
		struct ReplicaAssignment: Encodable {
			let partition: Int32
			let replicas: Int32
		}
		
		struct ConfigEntry: Encodable {
			let configName: String
			let configValue: String
		}
	}
	
	struct Response: KafkaResponse {
		struct TopicError: Encodable {
			let topic: String
			let errorCode: Int16
			let errorMessage: String
		}
	}
	
}
