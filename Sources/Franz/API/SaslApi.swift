//
//  SaslApi.swift
//  Franz
//
//  Created by Luke Lau on 15/04/2018.
//

import Foundation

struct SaslHandshakeRequest: KafkaRequest {
	
	typealias Response = SaslHandshakeResponse
	
	var apiKey: ApiKey { return .saslHandshake }
	var apiVersion: ApiVersion { return .v1 }
	
	let value: KafkaType?
	init(mechanism: String) {
		self.value = mechanism
	}
}

struct SaslHandshakeResponse: KafkaResponse {
	let errorCode: Int16
	let enabledMechanisms: [String]
	
	init(data: inout Data) {
		errorCode = Int16(data: &data)
		enabledMechanisms = [String](data: &data)
	}
}

struct SaslAuthenticateRequest: KafkaRequest {
	
	typealias Response = SaslAuthenticateResponse
	
	var apiKey: ApiKey { return .saslAuthenticate }
	var apiversion: ApiVersion { return .v0 }
	
	let value: KafkaType?
	init(saslAuthBytes: Data) {
		self.value = saslAuthBytes
	}
}

struct SaslAuthenticateResponse: KafkaResponse {
	let errorCode: Int16
	let errorMessage: String?
	let saslAuthBytes: Data
	
	init(data: inout Data) {
		errorCode = Int16(data: &data)
		errorMessage = String(data: &data)
		saslAuthBytes = Data(data: &data)
	}
}
