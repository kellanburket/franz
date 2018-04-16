//
//  KafkaRequest.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

protocol KafkaRequest {
	var apiKey: ApiKey { get }
	var apiVersion: ApiVersion { get }
	var value: KafkaType? { get }
	associatedtype Response: KafkaResponse
}

extension KafkaRequest {
	
	var apiVersion: ApiVersion { return .defaultVersion }
	
	func data(correlationId: Int32, clientId: String) -> Data {
		let content: [KafkaType?] = [apiKey, apiVersion, correlationId, clientId, value]
		let size: Int32 = content
			.compactMap { $0?.dataLength }
			.map(Int32.init)
			.reduce(0, +)

		return ([size] + content)
			.compactMap { $0?.data }
			.reduce(Data(), +)
	}
	
}
