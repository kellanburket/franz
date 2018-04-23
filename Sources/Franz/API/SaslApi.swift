//
//  SaslApi.swift
//  Franz
//
//  Created by Luke Lau on 15/04/2018.
//

import Foundation

struct SaslHandshakeRequest: KafkaRequest {
	
	static let apiKey: ApiKey = .saslHandshake 
	static let apiVersion: ApiVersion = 1
	
	let values: [KafkaType]
	init(mechanism: String) {
		self.values = [mechanism]
	}
	
	struct Response: KafkaResponse {
		let errorCode: Int16
		let enabledMechanisms: [String]
		
		init(data: inout Data) {
			errorCode = Int16(data: &data)
			enabledMechanisms = [String](data: &data)
		}
	}
}

struct SaslAuthenticateRequest: KafkaRequest {
	
	static let apiKey: ApiKey = .saslAuthenticate 
	static let apiVersion: ApiVersion = 0
	
	let values: [KafkaType]
	init(saslAuthBytes: Data) {
		self.values = [saslAuthBytes]
	}
	
	struct Response: KafkaResponse {
		let errorCode: Int16
		let errorMessage: String?
		let saslAuthBytes: Data
		
		init(data: inout Data) {
			errorCode = Int16(data: &data)
			errorMessage = String(data: &data)
			saslAuthBytes = Data(data: &data)
		}
	}
}
