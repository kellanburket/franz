//
//  Topic.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

public typealias TopicName = String

public struct Topic {
    public let name: TopicName
    public let partitions: [Int32]
}

internal struct KafkaTopic: Codable {
    let error: KafkaErrorCode
	let name: String
    private let partitionMetadata: [Partition]
    
    var partitions: [Int32: Partition] {
		return [Int32:Partition](uniqueKeysWithValues: partitionMetadata.map { ($0.id, $0) })
    }
    
    init(errorCode: Int, name: String, partitionMetadata: [Partition]) {
		self.error = KafkaErrorCode(rawValue: Int16(errorCode))!
        self.name = name
        self.partitionMetadata = partitionMetadata
    }
}
