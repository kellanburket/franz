//
//  APIVersionsAPI.swift
//  Franz
//
//  Created by Luke Lau on 20/04/2018.
//

import Foundation

struct ApiVersionsRequest: KafkaRequest {
	
	static let apiKey: ApiKey = .apiVersions
	static var apiVersion: ApiVersion { return 1 }
	
	var values: [KafkaType] { return [] }
	
	struct Response: KafkaResponse {
		init(data: inout Data) {
			errorCode = Int16(data: &data)
			apiVersions = [ApiVersion](data: &data)
			throttleTime = Int32(data: &data)
		}
		
		/// Response error code
		let errorCode: Int16
		/// API versions supported by the broker
		let apiVersions: [ApiVersion]
		/// Duration in milliseconds for which the request was throttled due to quota violation (Zero if the request did not violate any quota)
		let throttleTime: Int32
		
		struct ApiVersion: KafkaType {
			init(data: inout Data) {
				apiKey = Int16(data: &data)
				maxVersion = Int16(data: &data)
				minVersion = Int16(data: &data)
			}
			
			var data: Data {
				return [apiKey, maxVersion, minVersion].map { $0.data }.reduce(Data(), +)
			}
			var dataLength: Int {
				return [apiKey, maxVersion, minVersion].map { $0.dataLength }.reduce(0, +)
			}
			
			/// API key
			let apiKey: Int16
			/// Minimum supported version
			let maxVersion: Int16
			/// Maximum supported version
			let minVersion: Int16
		}
	}
}
