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
	
	var values: [KafkaType] { return [requests, timeout, validateOnly] }
	
	let requests: [CreateTopicRequest]
	let timeout: Int32
	let validateOnly: Bool
	
	init(requests: [CreateTopicRequest], timeout: Int32 = 0, validateOnly: Bool = false) {
		self.requests = requests
		self.timeout = timeout
		self.validateOnly = validateOnly
	}
	
	struct CreateTopicRequest: CompoundKafkaType {
		let topic: String
		let numPartitions: Int32
		let replicationFactor: Int16
		let replicaAssignments: [ReplicaAssignment]
		let configEntries: [ConfigEntry]
		
		init(topic: String, numPartitions: Int32, replicationFactor: Int16, configEntries: [String: String] = [:]) {
			self.topic = topic
			self.numPartitions = numPartitions
			self.replicationFactor = replicationFactor
			self.replicaAssignments = []
			self.configEntries = configEntries.map(ConfigEntry.init)
		}
		
		init(topic: String, replicaAssignments: [Int32: Int32], configEnties: [String: String] = [:]) {
			self.topic = topic
			self.numPartitions = -1
			self.replicationFactor = -1
			self.replicaAssignments = replicaAssignments.map(ReplicaAssignment.init)
			self.configEntries = configEnties.map(ConfigEntry.init)
		}
		
		init(data: inout Data) {
			topic = String(data: &data)
			numPartitions = Int32(data: &data)
			replicationFactor = Int16(data: &data)
			replicaAssignments = [ReplicaAssignment](data: &data)
			configEntries = [ConfigEntry](data: &data)
		}
		
		var values: [KafkaType] {
			return [topic, numPartitions, replicationFactor, replicaAssignments, configEntries]
		}
		
		struct ReplicaAssignment: CompoundKafkaType {
			let partition: Int32
			let replicas: Int32
			
			var values: [KafkaType] { return [partition, replicas] }
		}
		
		struct ConfigEntry: CompoundKafkaType {
			let configName: String
			let configValue: String
			
			var values: [KafkaType] { return [configName, configValue] }
		}
	}
	
	struct Response: KafkaResponse {
		
		let throttleTime: Int32
		let topicErrors: [TopicError]
		
		struct TopicError: KafkaType {
			init(data: inout Data) {
				topic = String(data: &data)
				errorCode = Int16(data: &data)
				errorMessage = String(data: &data)
			}
			
			var values: [KafkaType] {
				return [topic, errorCode, errorMessage]
			}
			
			var data: Data {
				return values.map { $0.data }.reduce(Data(), +)
			
			}
			
			var dataLength: Int {
				return values.map { $0.dataLength }.reduce(0, +)
			}
			
			let topic: String
			let errorCode: Int16
			let errorMessage: String
		}
		
		init(data: inout Data) {
			throttleTime = Int32(data: &data)
			topicErrors = [TopicError](data: &data)
		}
		
	}
	
}

// Use extensions for these inits so that we get the default syntehsized one too
extension CreateTopicsRequest.CreateTopicRequest.ReplicaAssignment {
	init(data: inout Data) {
		partition = Int32(data: &data)
		replicas = Int32(data: &data)
	}
}


extension CreateTopicsRequest.CreateTopicRequest.ConfigEntry {
	init(data: inout Data) {
		configName = String(data: &data)
		configValue = String(data: &data)
	}
}
