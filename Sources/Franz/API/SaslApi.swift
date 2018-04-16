//
//  SaslApi.swift
//  Franz
//
//  Created by Luke Lau on 15/04/2018.
//

import Foundation

class SaslHandshakeRequest: KafkaRequest, AssociatedResponse {
	
	typealias Response = SaslHandshakeResponse
	
	let mechanism: String
	
	init(mechanism: String) {
		self.mechanism = mechanism
		super.init(apiKey: .saslHandshake, value: mechanism, apiVersion: .v1)
	}
}

class SaslHandshakeResponse: KafkaResponse {
	let errorCode: Int16
	let enabledMechanisms: [String]
	
	required init(data: inout Data) {
		errorCode = Int16(data: &data)
		enabledMechanisms = [String](data: &data)
	}
}

class SaslAuthenticateRequest: KafkaRequest, AssociatedResponse {
	
	typealias Response = SaslAuthenticateResponse
	
	let saslAuthBytes: Data
	
	init(saslAuthBytes: Data) {
		self.saslAuthBytes = saslAuthBytes
		super.init(apiKey: .saslAuthenticate, value: saslAuthBytes, apiVersion: .v0)
	}
}

class SaslAuthenticateResponse: KafkaResponse {
	let errorCode: Int16
	let errorMessage: String?
	let saslAuthBytes: Data
	
	required init(data: inout Data) {
		errorCode = Int16(data: &data)
		errorMessage = String(data: &data)
		saslAuthBytes = Data(data: &data)
	}
}
