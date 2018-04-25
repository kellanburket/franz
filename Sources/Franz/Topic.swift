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

internal struct KafkaTopic: KafkaType {
	let name: String
    private let errorCode: Int16
    private let partitionMetadata: [Partition]
    
    var error: KafkaErrorCode? {
        if let error = KafkaErrorCode(rawValue: errorCode) {
            return error
        } else {
            return nil
        }
    }
    
    var partitions: [Int32: Partition] {
		return [Int32:Partition](uniqueKeysWithValues: partitionMetadata.map { ($0.id, $0) })
    }
    
    init(errorCode: Int, name: String, partitionMetadata: [Partition]) {
        self.errorCode = Int16(errorCode)
        self.name = name
        self.partitionMetadata = partitionMetadata
    }
    
    init(data: inout Data) {
        errorCode = Int16(data: &data)
        name = String(data: &data)
        partitionMetadata = [Partition](data: &data)
    }
    
	var dataLength: Int {
        return self.errorCode.dataLength
    }
    
    var data: Data {
        return errorCode.data + name.data + partitionMetadata.data
    }
}
