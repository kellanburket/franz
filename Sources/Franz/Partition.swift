//
//  Partition.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

typealias PartitionId = Int32

struct Partition: Codable {
	
    private let partitionErrorCode: Int16
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: partitionErrorCode)
    }
    
    let id: PartitionId
    
    let leader: PartitionId
    
    let replicas: [Int32]
    
    let isr: [Int32]
}
