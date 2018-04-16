//
//  KafkaRequest.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

/// A Kafka API request.
protocol KafkaRequest {
	/// The API key to represent the type of request.
	///
	/// https://kafka.apache.org/protocol.html#protocol_api_keys
	var apiKey: ApiKey { get }
	
	/// The API version. Defaults to 0.
	var apiVersion: ApiVersion { get }
	
	/// The content of the request.
	var value: KafkaType? { get }
	
	/// The type of response you expect to receive when making this request.
	associatedtype Response: KafkaResponse
}

extension KafkaRequest {
	
	var apiVersion: ApiVersion { return .defaultVersion }
	
	/// The request header and content data.
	/// - Parameter correlationId: A unique correlation ID to associate with the response.
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
