//
//  GroupCoordinatorAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

struct GroupCoordinatorRequest: KafkaRequest {
	
	let values: [Encodable]
    
    init(id: String) {
		self.values = [id]
    }
	
	static let apiKey: ApiKey = .groupCoordinatorRequest
	static let apiVersion: ApiVersion = 0
	
	struct Response: Decodable {
		let error: KafkaErrorCode
		let id: Int32
		let host: String
		let port: Int32
	}
    
}
